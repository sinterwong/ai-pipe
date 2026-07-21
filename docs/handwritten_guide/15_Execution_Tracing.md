# 15. 高效执行轨迹追踪 (ChromeTraceSink)

在复杂的 DAG 并发管线中，虽然有了 `LatencyHistogram` 这样强大的聚合可观测数字，但当系统偶尔发生突发性延迟（Spike）时，单纯的百分位数就显得无能为力了。
开发者必须看清**具体哪一帧在哪个 Worker 线程上发生了阻塞、什么时候入队、什么时候被调度执行、 process 耗时了多久**。

AI Pipe 设计了一套 **热路径极低开销（Hot-Path Ultra-Low Cost）** 的 Trace 埋点系统，可以直接导出完美的、能被 Chrome Perfetto 直接打开进行时间线切片的 JSON 轨迹文件。

---

## 1. 核心设计原理

### 1.1 四大核心生命周期事件埋点
为了无缝还原任意数据帧在微内核引擎中的一生，我们在引擎的 PIMPL 实现中，挑选了 4 个最核心的生命周期交界点进行打桩埋点：

| 事件 Phase | 物理埋点位置 | 携带的诊断参数信息 | 物理意义 |
|---|---|---|---|
| **`Enqueue`** | 数据进入节点输入队列。 | `frame_id`, `stream_id`, `port_name` | 标志着数据流在当前节点的起点（背压等待在此阶段产生）。 |
| **`Schedule`** | 调度器评估通过，向线程池提交任务。 | `node_name`, `decision` | 标志着从“数据就绪”到“线程领走”之间的**调度延迟（Schedule Latency）**。 |
| **`Execute`** | 节点 `process()` 函数被 Worker 线程拉起开始执行。 | `thread_id` | 标志着真正的物理计算起点。 |
| **`Propagate`** | 节点 `process()` 运行结束，输出数据路由给下游。 | `elapsed_time` | 标志着物理计算的终点。 |

### 1.2 零开销指针判空桥接
在高性能热路径上，绝不能因为 Trace 的存在而产生任何不必要的内存拷贝或字符串格式化。

**实现巧思**：
1.  **极简判空前置**：在引擎的核心循环里，只有当用户显式注入了 `ITraceSink` 时，才去构造 `TraceEvent` 结构体：
    ```cpp
    if (m_traceSink) {
        m_traceSink->onEvent(TraceEvent{...});
    }
    ```
    未开启 Trace 时，每个埋点仅仅花费一次 **CPU 分支预测 100% 命中的指针判空（Null check）**，耗时小于 0.2ns，完全不留痕迹。
2.  **String_View 局部生命周期契约**：
    `TraceEvent` 结构体中所有的字符串（如 `node_name`, `port_name`）全部声明为 `std::string_view`，**坚决不持有 std::string 对象**。这样避免了高频产生临时字符串造成的堆内存分配，字符串直接指向拓扑图中的常驻静态物理内存。

---

## 2. 核心巧思与实现细节

### 2.1 互斥锁缓冲区的 Chrome Trace JSON 格式化导出
虽然 `ITraceSink` 的 `onEvent` 接口是在高并发的 Worker 线程上被并发调用的，但要生成合法的 JSON 格式，必须解决多线程乱序写入问题。

**实现巧思（`ChromeTraceSink`）**：
1.  **高效缓冲设计**：
    `ChromeTraceSink` 内部维护一个带轻量互斥锁的 vector 数组：
    ```cpp
    struct ChromeEvent {
        std::string name;
        std::string cat;
        char ph; // 'B' (Begin) 或 'E' (End) 或 'i' (Instant)
        uint64_t ts; // 微秒时间戳
        uint32_t pid;
        uint32_t tid;
        std::string args;
    };
    std::vector<ChromeEvent> m_events;
    ```
2.  **双阶段并发友好**：
    *   **热路径阶段**：当 `onEvent` 触发时，获取 `std::unique_lock`，仅进行极速的 `push_back`。不在这里做任何耗时的 JSON 格式化或写磁盘操作，保持对 Worker 热路径线程的极小打扰。
    *   **离线导出阶段**：当管线停止（IDLE）后，调用 `writeFile(filename)`。在离线状态下，将 vector 里的事件，完美组装成 Chrome Trace Event Format 标准规范：
        ```json
        [
          {"name": "Execute_preprocess", "cat": "Node", "ph": "B", "ts": 1234567, "pid": 1, "tid": 101},
          {"name": "Execute_preprocess", "cat": "Node", "ph": "E", "ts": 1234600, "pid": 1, "tid": 101}
        ]
        ```
    这可以直接导入到 `chrome://tracing` 或 https://ui.perfetto.dev 进行完美切片还原。

---

## 3. 手搓实现参考骨架

你可以根据以下完美契合 Chrome UI 的 Trace 系统进行手搓复习：

```cpp
enum class TracePhase {
    Enqueue,
    Schedule,
    Execute,
    Propagate
};

struct TraceEvent {
    TracePhase phase;
    std::string_view nodeName;
    std::string_view portName;
    FrameId frameId;
    StreamId streamId;
    std::chrono::steady_clock::time_point timestamp;
};

class ITraceSink {
public:
    virtual ~ITraceSink() = default;
    virtual void onEvent(const TraceEvent& event) = 0;
};

// ==================== Chrome Trace 导出器 ====================
class ChromeTraceSink : public ITraceSink {
public:
    void onEvent(const TraceEvent& event) override {
        auto now = std::chrono::steady_clock::now();
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
        auto tid = static_cast<uint32_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));

        std::lock_guard<std::mutex> lock(m_mutex);

        // 映射为 Chrome 'ph' (Phase) 规范：B=Begin, E=End, I=Instant
        if (event.phase == TracePhase::Execute) {
            m_events.push_back({"process", "Execute", 'B', us, tid, event.nodeName, event.frameId});
        } else if (event.phase == TracePhase::Propagate) {
            m_events.push_back({"process", "Execute", 'E', us, tid, event.nodeName, event.frameId});
        } else {
            m_events.push_back({"enqueue", "Queue", 'i', us, tid, event.nodeName, event.frameId});
        }
    }

    void writeFile(const std::string& filepath) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::ofstream out(filepath);
        out << "[\n";
        for (size_t i = 0; i < m_events.size(); ++i) {
            const auto& ev = m_events[i];
            out << "  {"
                << "\"name\": \"" << ev.name << "\", "
                << "\"cat\": \"" << ev.cat << "\", "
                << "\"ph\": \"" << ev.ph << "\", "
                << "\"ts\": " << ev.ts << ", "
                << "\"pid\": 1, "
                << "\"tid\": " << ev.tid << ", "
                << "\"args\": {\"node\": \"" << ev.nodeName << "\", \"frame\": " << ev.frameId << "}"
                << "}" << (i + 1 < m_events.size() ? ",\n" : "\n");
        }
        out << "]\n";
    }

private:
    struct InternalEvent {
        std::string name;
        std::string cat;
        char ph;
        int64_t ts;
        uint32_t tid;
        std::string nodeName;
        FrameId frameId;
    };

    std::mutex m_mutex;
    std::vector<InternalEvent> m_events;
};
```

手搓 `ChromeTraceSink` 期间，你将切身体会到“热路径非侵入式设计（Zero Allocation, String View）”的无上美学。当你在 Perfetto UI 界面上，拉出你手搓管线那无比整齐、五彩斑斓的线程调度执行时间线时，你将感受到难以名状的工程自豪。
