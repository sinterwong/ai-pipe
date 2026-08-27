# 节点开发指南

本文是编写 AI Pipe 处理节点的规范参考（适用于 v0.4.0+）。

---

## 1. 最小节点

```cpp
#include <ai_pipe/ai_pipe.hpp>

class ResizeNode : public ai_pipe::ILogicNode {
public:
  explicit ResizeNode(const std::string &name) : ILogicNode(name) {}

  void process(const ai_pipe::PortDataMap &inputs,
               ai_pipe::PortDataMap &outputs,
               std::shared_ptr<ai_pipe::PipelineContext> ctx) override {
    auto in = inputs.at("input");              // 只读共享包
    auto out = std::make_shared<ai_pipe::PortData>();
    out->setParam("image", resize(in->param<cv::Mat>("image").value()));
    outputs["output"] = out;                   // 交接后不可再改
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }
};
```

要点：

- **端口必须声明**。引擎按 `getExpectedInputPorts()` 为每个输入端口建
  独立队列；未声明的端口无法连接。
- **`process()` 会被并发调度框架从 worker 线程调用**，但同一节点同一
  时刻只有一个 `process()` 在执行（EXECUTING 状态机保证）。节点内部
  成员无需加锁，除非节点自己暴露给外部线程。
- **抛出异常是允许的错误通道**：引擎捕获并转为 `NodeException`。流模式
  下异常仅影响当前帧，节点随后继续服务；批模式下终止本次运行。

## 2. 数据包与所有权

- 收到的包（`PortDataPtr`）是 **`const`** 的——并行分支共享同一对象。
- 新建输出：`std::make_shared<PortData>()`，填充后放入 `outputs`。
- 确需"修改输入"：`auto copy = ai_pipe::mutableCopy(packet);`（显式 COW）。
- **不要**缓存收到的包并在 `process()` 返回后异步修改。

### 参数访问（单通路：Result）

```cpp
// 唯一取参通路：全程无异常
auto mat = packet->param<cv::Mat>("image");
if (!mat) { /* mat.error() 带上下文（缺键/类型不匹配均为 InvalidArgument） */ }

// 带默认值
double thr = packet->param<double>("threshold").valueOr(0.5);

// 声明式（键+类型只写一次）
static inline const ai_pipe::TypedParam<cv::Mat>
    k_image{"image"};
auto mat = k_image.read(*packet);   // Result<cv::Mat>
```

早期的抛异常接口（`getParam`/`getOptionalParam`/`TypedParam::get/tryGet`）
已移除。节点 `process()` 内部**抛异常仍然合法**——那是引擎的防御边界
（转为 `NodeException`），不是取参 API 的错误通道。

### 帧标识

`packet->id`（FrameId）、`stream_id`、`timestamp` 由引擎在入口自动盖章
（`id==0` 视为未分配），输出包自动继承输入的帧标识。**多输入节点依赖
FrameId 做对齐**——除非有明确理由，不要手工改写它。

## 3. 生命周期

```cpp
ai_pipe::Result<void>
setup(std::shared_ptr<ai_pipe::PipelineContext> ctx) override {
  m_model = loadModel(ctx->getConfig<std::string>("model_path"));
  if (!m_model) {
    return ai_pipe::Result<void>::err(ai_pipe::ErrorCode::NodeException,
                                      "model load failed", getName());
  }
  return ai_pipe::Result<void>::ok();
}

void teardown() noexcept override { m_model.reset(); }
```

- `setup()` 在首次执行前按拓扑序调用一次；失败会中止运行并按逆序
  teardown 已成功的节点。
- `teardown()` 在 `reset()`/引擎析构时逆序调用，此时保证无在途任务。
  **不得抛异常。**

### 3.1 流结束时的 flush — `onEndOfStream()`（R6.1）

**攒批、时域平滑、滑窗聚合等带内部缓冲的节点必须实现这个钩子**，否则
处理有限输入（播完的视频文件）时，最后不足一批的数据会永久留在节点
内部：

```cpp
void process(const PortDataMap &in, PortDataMap &out,
             std::shared_ptr<PipelineContext>) override {
  m_pending.push_back(in.at("input"));
  if (m_pending.size() == k_batch) out["output"] = runBatch();
}

void onEndOfStream(PortDataMap &out,
                   std::shared_ptr<PipelineContext>) override {
  if (!m_pending.empty()) out["output"] = runBatch();  // 残留批次
}
```

- 每轮运行**至多调用一次**，在该节点全部输入端口排空之后、EOS 向下游
  传播之前。
- 引擎保证它**不与 `process()` 并发**，可以自由访问节点内部状态。
- 写入 `outputs` 的包与 `process()` 的输出同路传播（继承帧身份、入下游
  队列、sink 则计入结果）；无残留就留空。
- 抛异常会被记为节点失败并上报，但**不阻断** EOS 传播。

由外部通过 `Pipeline::signalEndOfStream(node, port)` 触发；详见
`docs/design/eos_flush.md`。

## 4. 端口类型声明（可选但推荐）

```cpp
std::type_index portPayloadType(const std::string &port) const override {
  if (port == "output") return typeid(cv::Mat);
  if (port == "input")  return typeid(cv::Mat);
  return typeid(void);   // 未声明 = 不校验
}
```

两端都声明类型且不匹配的 `addEdge` 会在建图期被拒绝，而不是运行时
`bad_any_cast`。

## 5. 注册与配置驱动构建（可选）

```cpp
// 节点 .cpp 中（命名空间作用域）：
AI_PIPE_REGISTER_NODE(ResizeNode);                 // ctor(name)
AI_PIPE_REGISTER_NODE_WITH_CONFIG(DetectorNode);   // ctor(name, config)

// 装配侧：
auto node = ai_pipe::NodeRegistry::instance()
                .create("DetectorNode", "det0", config);  // Result
```

动态插件不要使用上述静态注册宏；应导出显式注册入口，并为节点分配稳定、
与 C++ 类名解耦的类型 ID。插件协议、CMake 辅助函数和发现路径见
[`Plugin_Guide.md`](Plugin_Guide.md)。

## 6. 上下文（PipelineContext）

- 资源/服务：`ctx->setResource` / `ctx->getService<T>()`（线程安全）。
- 取消：`Pipeline::cancel()` 与 `run(timeout)` 超时
  都会触发 context 的 `CancellationToken`；引擎在调度点将其与 stopFlag
  等效对待——已取消后不再调度新节点。**长任务节点应在内部循环里协作式
  检查** `ctx->isCancellationRequested()`（或
  `ctx->cancellation().isCancelled()`），发现取消后尽快返回（部分输出会
  被丢弃，不必清理下游）。不检查也不会破坏正确性，只是取消要等到当前
  `process()` 自然结束才生效。token 在每次执行/流启动时自动复位。
- 日志：节点内用 `ctx->logInfo(getName(), ...)`；若希望框架日志也进入
  你的 logger，调用 `ctx->attachEngineLogs()`。
- `ctx` 可能为 `nullptr`（直接驱动引擎且未传 context 时）——访问前判空。

## 7. 性能守则

- `process()` 里避免不必要的堆分配；大负载放 `shared_ptr` 参数传递
  （包本身即是零拷贝共享）。
- 声明的端口数决定调度检查成本；不要声明用不到的端口。
- 慢节点（>数百 µs）考虑配小容量 DropHead 队列承接背压，而不是让
  上游无界积压。
