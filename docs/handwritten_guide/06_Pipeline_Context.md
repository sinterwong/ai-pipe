# 06. 管线全局上下文 (PipelineContext)

在高度并发且动态执行的 DAG（有向无环图）管线中，各个计算节点并不是孤立存在的。它们需要共享模型句柄（如推理引擎）、读写配置参数、收集自身性能指标、响应管线中止通知，甚至是与进程级日志进行低延迟交互。

`PipelineContext` 扮演了整个管线的“中枢神经系统”，以 100% 线程安全和高效读写设计连接起计算节点、监控层与宿主系统。

---

## 1. 核心设计原理

### 1.1 核心职责与三大保障机制
```
                           PipelineContext (Shared State)
                                     |
         +---------------------------+---------------------------+
         |                           |                           |
         v                           v                           v
+------------------+        +------------------+        +------------------+
| Resource/Service |        |Metrics Collection|        |Cancellation Token|
|  (Shared Mutex)  |        |  (Atomic/Lock)   |        | (Atomic Boolean) |
+------------------+        +------------------+        +------------------+
```

1.  **高吞吐资源管理（Resource & Service Locator）**：
    *   **职责**：注册与读取全局命名的任意对象（如 TensorRT 推理引擎、全局配置项）和强类型全局服务（如 LogService）。
    *   **保障**：由于管线在运行时频繁读取资源（Read-Heavy），而在初始化和退出时才写入（Write-Rare）。必须使用 **读写锁（Shared Mutex）**，实现多线程并发无缝读取，仅在动态注入或覆写资源时才产生短时的写独占。
2.  **毫秒级协作式取消（Cooperative Cancellation）**：
    *   **职责**：当管线触发 `cancel()` 或执行遇到严重故障（如 Batch 模式节点运行崩溃）时，管线应优雅地、迅速地通知所有正在运行的 Worker 线程立即返回。
    *   **保障**：采用基于 `std::atomic<bool>` 的 `CancellationToken`，使用高吞吐的 `acquire/release` 内存序。各节点在耗时计算的热循环中只需以极低的代价（单次 atomic load 耗时通常小于 2ns）低频轮询，发现取消请求后迅速释放资源安全返回，完美避免了硬性杀死线程造成的物理死锁或资源泄漏。
3.  **非占有式指标追踪（Scoped Node Metrics Execution）**：
    *   **职责**：自动搜集每个节点在每次执行时的开始/结束时间戳、耗时、成功率、以及产生的输入输出计数，用作实时监控。
    *   **保障**：利用 C++ 的 **RAII（Resource Acquisition Is Initialization）** 机制设计 Scoped 辅助结构，在作用域析构时，将统计数据原子地累加到 `PipelineContext` 内部，防止节点因手动编写 `metrics.record()` 遗漏而引发的指标漂移。

---

## 2. 核心巧思与实现细节

### 2.1 基于 `std::any` 的类型擦除与安全转换
如何能够在资源定位器中存储任何自定义的用户对象？

**实现巧思**：
在 C++20 中，我们可以使用 `std::any` 作为通用的类型容器。它的底层通常能实现短类型优化（SBO，Small Buffer Optimization）。为了支持多态安全，我们更常存储 `std::shared_ptr<void>`：
```cpp
mutable std::shared_mutex m_resourceMutex;
std::unordered_map<std::string, std::any> m_resources;
```
*   **写入路径**：获取写锁 `std::unique_lock`，将用户传入的 `shared_ptr<T>` 擦除类型存入。
*   **读取路径**：获取读锁 `std::shared_lock`。借助 `std::any_cast<std::shared_ptr<T>>` 强类型地安全取出。如果类型不匹配，`any_cast` 会抛出异常，或者采用无异常的安全 `Result<std::shared_ptr<T>>` 返回（这也是 AI Pipe 极度推荐的设计）。

### 2.2 线程安全的 Service 注册机制
除了一般的命名键值 `Resource`，框架还需要支持按“强类型（Interface Type）”作为键的 Service Locator。

**实现巧思**：
利用 `std::type_index`：
```cpp
std::unordered_map<std::type_index, std::shared_ptr<void>> m_services;
```
由于每个类在 C++ 中都有全局唯一的 `typeid`，我们可以直接：
```cpp
template <typename T>
void setService(std::shared_ptr<T> service) {
    std::unique_lock<std::shared_mutex> lock(m_serviceMutex);
    m_services[std::type_index(typeid(T))] = std::static_pointer_cast<void>(service);
}
```
通过该设计，调用方只需调用 `ctx->getService<IInferenceEngine>()`，就能在 O(1) 的时间内，零类型转换损耗地定位到具体的全局单例服务。

### 2.3 Scoped 节点辅助计时器（RAII Timer）
如果每次节点执行都手动记录开始时间，并在结束时累加指标，代码中将会充斥大量的冗余行，且一旦在处理中途由于 `return` 提前返回，指标搜集就会彻底失效。

**实现巧思**：
设计一个 `ScopedNodeExecution` 守护类：
```cpp
class ScopedNodeExecution {
public:
    ScopedNodeExecution(std::shared_ptr<PipelineContext> ctx, std::string nodeName)
        : m_ctx(std::move(ctx)), m_nodeName(std::move(nodeName)),
          m_startTime(std::chrono::steady_clock::now()) {
        m_ctx->beginNodeExecution(m_nodeName);
    }

    ~ScopedNodeExecution() {
        auto endTime = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(endTime - m_startTime);
        m_ctx->endNodeExecution(m_nodeName, m_success, elapsed.count());
    }

    void setSuccess(bool success) { m_success = success; }

private:
    std::shared_ptr<PipelineContext> m_ctx;
    std::string m_nodeName;
    std::chrono::steady_clock::time_point m_startTime;
    bool m_success{false};
};
```
在节点 `process` 内部，仅需在入口处声明：
`ScopedNodeExecution scope(context, getName());`
随后，即使节点有 10 个提前返回的 `if (!data) return;` 分支，当函数栈销毁、作用域结束时，`scope` 析构函数将被编译器 100% 强制触发，安全地搜集到真实的执行耗时和成功状态。

---

## 3. 手搓实现参考骨架

你可以参照以下高度抽象的优秀 C++20 设计来复写这一核心中枢：

```cpp
class CancellationToken {
public:
    CancellationToken() : m_canceled(false) {}

    void requestCancellation() {
        m_canceled.store(true, std::memory_order_release);
    }

    bool isCancellationRequested() const {
        return m_canceled.load(std::memory_order_acquire);
    }

    void reset() {
        m_canceled.store(false, std::memory_order_release);
    }

private:
    std::atomic<bool> m_canceled;
};

class PipelineContext : public std::enable_shared_from_this<PipelineContext> {
public:
    PipelineContext() = default;

    // 1. 线程安全的共享资源注册
    template <typename T>
    void setResource(const std::string& name, std::shared_ptr<T> resource) {
        std::unique_lock<std::shared_mutex> lock(m_resourceMutex);
        m_resources[name] = std::any(std::move(resource));
    }

    template <typename T>
    std::shared_ptr<T> getResource(const std::string& name) const {
        std::shared_lock<std::shared_mutex> lock(m_resourceMutex);
        auto it = m_resources.find(name);
        if (it == m_resources.end()) return nullptr;

        try {
            return std::any_cast<std::shared_ptr<T>>(it->second);
        } catch (const std::bad_any_cast&) {
            return nullptr;
        }
    }

    // 2. 按类型索引的 Service 注册
    template <typename T>
    void setService(std::shared_ptr<T> service) {
        std::unique_lock<std::shared_mutex> lock(m_serviceMutex);
        m_services[std::type_index(typeid(T))] = std::static_pointer_cast<void>(service);
    }

    template <typename T>
    std::shared_ptr<T> getService() const {
        std::shared_lock<std::shared_mutex> lock(m_serviceMutex);
        auto it = m_services.find(std::type_index(typeid(T)));
        if (it == m_services.end()) return nullptr;
        return std::static_pointer_cast<T>(it->second);
    }

    // 3. 协作式取消管理
    CancellationToken& cancellation() { return m_cancellation; }
    const CancellationToken& cancellation() const { return m_cancellation; }

    // 4. 原子指标更新
    void beginNodeExecution(const std::string& nodeName) {
        // 更新原子计数器
    }

    void endNodeExecution(const std::string& nodeName, bool success, int64_t elapsedUs) {
        // 更新原子执行时间、成功率等指标
    }

private:
    mutable std::shared_mutex m_resourceMutex;
    std::unordered_map<std::string, std::any> m_resources;

    mutable std::shared_mutex m_serviceMutex;
    std::unordered_map<std::type_index, std::shared_ptr<void>> m_services;

    CancellationToken m_cancellation;
};
```

手搓 `PipelineContext` 期间，深刻体会“数据并发隔离机制（读多写少用 Shared Mutex，单标记控制用 Atomic Boolean）”的奥妙。这是维持一个高性能、低延迟系统的精妙基石。
