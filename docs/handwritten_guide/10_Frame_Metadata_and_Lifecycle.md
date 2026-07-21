# 10. 帧元数据与流生命周期 (IFrameMetadata)

当 AI Pipe 运行在 `STREAM`（实时流式）模式下时，管线处理的不再是离线的孤立单张图片，而是来自多个独立传感器或视频源、源源不断的、高频且具有严密时间属性的“帧序列”。

为了在多路并发分支之间实现精确的帧对齐、检测数据滞后、进行协同丢帧、并在推流结束时优雅释放，管线流动的数据包 `DataPacket` 必须绑定并携带高吞吐的 **帧元数据（Frame Metadata）**。

---

## 1. 核心设计原理

### 1.1 帧元数据生命周期模型
```
  [Producer Side]                          [Engine Pipeline]                      [Sink Side]
         |                                         |                                   |
(Generate Packet)                                  |                                   |
         |                                         |                                   |
         v                                         |                                   |
 PortDataPtr + BasicFrameMetadata (Frame N)        |                                   |
         |                                         |                                   |
         +-------------(pushInput)---------------->|                                   |
                                                   |                                   |
                                            (Track Frame N)                            |
                                            (Align & Process)                          |
                                                   |                                   |
                                                   +-------------(deliver)------------>|
                                                                                       |
                                                                                (Sink completes)
                                                                                (Destruct Packet)
```

1.  **标识符规范**：
    每帧必须持有三个最核心的身份印章：
    *   `FrameId`：流内绝对单调递增的无符号 64 位整数（`uint64_t`）。
    *   `StreamId`：标识当前数据来自于哪一路物理输入源（`uint32_t`，用于多路视频流汇合对齐）。
    *   `Timestamp`：记录该帧被采集或输入时的物理/系统高精度时间戳（`std::chrono::steady_clock::time_point`）。
2.  **特殊的流生命周期边界**：
    一个健壮的流式管线必须能够识别数据流的“开端”与“终止”。
    *   `k_invalid_frame_id = 0`：标志着包未被赋予有效的帧属性，不参与同步。
    *   `k_end_of_stream_frame_id = UINT64_MAX`：标志着**流结束（EOS, End of Stream）**特殊占位符。当各节点输入队列收到该帧号时，意味着上游数据已全部发送完毕，节点必须立刻执行数据收尾、将缓存中的残留数据（如视频编码器尾包）全部 Drained 并发布，然后向下游传递该 EOS 标志。

---

## 2. 核心巧思与实现细节

### 2.1 帧元数据接口解耦设计
因为 AI Pipe 追求极致的扩展性，帧同步不应该只局限于“帧号必须严格相等”这种死板的教条。对于一些网络摄像头，丢包和网络延迟会导致各路采集到的 FrameId 天生错位，但它们采集时的**物理时间戳（Timestamp）**是极度相近的。

**实现巧思**：
设计纯虚抽象 `IFrameMetadata` 接口：
```cpp
class IFrameMetadata {
public:
    virtual ~IFrameMetadata() = default;

    virtual uint64_t frameId() const = 0;
    virtual uint32_t streamId() const = 0;
    virtual std::chrono::steady_clock::time_point timestamp() const = 0;

    // 对比判定接口：交由具体的同步子类去裁定两帧是否可以对齐
    virtual bool shouldSyncWith(const IFrameMetadata& other) const = 0;
    virtual int compareTo(const IFrameMetadata& other) const = 0;
    virtual std::unique_ptr<IFrameMetadata> clone() const = 0;
};
```
*   **精确匹配实现（`BasicFrameMetadata`）**：两个包的 `frameId` 必须严格一致。适用于单视频源，或上游已做硬件帧对齐的极佳场景。
*   **容差匹配实现（`TimestampFrameMetadata`）**：两个包的 `frameId` 可以不同，但它们的 `timestamp` 物理差值必须小于设定的阈值（例如 33ms，对应 30FPS 视频的一帧间隔）。适用于无硬件对齐、纯软件配对的多传感器融合系统。

### 2.2 自动盖章与单调性保护
如果让用户每次在外部手动计算、分配 `FrameId` 和 `Timestamp`，不仅繁琐，而且一旦用户传入了非单调递增（Non-monotonic）的帧号，会导致多流对齐算法发生逻辑回溯、引发死循环或数据永远卡死在队列中。

**实现巧思**：
在 `Pipeline` 与 `ExecutionEngine` 的入口处，设计 **FrameMetadataFactory 盖章保护层**：
*   **引擎默认编号**：如果用户传入的 `PortDataPtr` 包含无效帧号（`id == 0`），引擎在接收到数据的第一瞬间，会通过内部的线程安全自增器（基于 `std::atomic<uint64_t>`，按 `stream_id` 物理分桶计数）为其**自动盖章（stampIncomingFrame）**，打上系统单调时间戳。
*   **水位线防御**：对已编号的包，引擎会校验并拦截“逆向推流”（即新帧的 FrameId 竟然比已处理的最新帧小），直接返回 `InvalidArgument`，将隐患彻底阻隔在内核之外。

---

## 3. 手搓实现参考骨架

你可以根据以下严密的 C++20 `IFrameMetadata` 接口与其内置实现进行手搓：

```cpp
using FrameId = uint64_t;
using StreamId = uint32_t;
using Timestamp = std::chrono::steady_clock::time_point;

constexpr FrameId k_invalid_frame_id = 0;
constexpr FrameId k_end_of_stream_frame_id = std::numeric_limits<FrameId>::max();

class IFrameMetadata {
public:
    virtual ~IFrameMetadata() = default;

    virtual FrameId frameId() const = 0;
    virtual StreamId streamId() const = 0;
    virtual Timestamp timestamp() const = 0;

    virtual bool shouldSyncWith(const IFrameMetadata& other, std::chrono::microseconds tolerance) const = 0;
    virtual int compareTo(const IFrameMetadata& other) const = 0;
    virtual std::unique_ptr<IFrameMetadata> clone() const = 0;
};

// ==================== 基础帧元数据实现 ====================
class BasicFrameMetadata : public IFrameMetadata {
public:
    BasicFrameMetadata(FrameId fid, StreamId sid, Timestamp ts)
        : m_frameId(fid), m_streamId(sid), m_timestamp(ts) {}

    FrameId frameId() const override { return m_frameId; }
    StreamId streamId() const override { return m_streamId; }
    Timestamp timestamp() const override { return m_timestamp; }

    bool shouldSyncWith(const IFrameMetadata& other, std::chrono::microseconds) const override {
        // 精确对齐：必须 FrameId 相等
        return m_frameId == other.frameId();
    }

    int compareTo(const IFrameMetadata& other) const override {
        if (m_frameId < other.frameId()) return -1;
        if (m_frameId > other.frameId()) return 1;
        return 0;
    }

    std::unique_ptr<IFrameMetadata> clone() const override {
        return std::make_unique<BasicFrameMetadata>(m_frameId, m_streamId, m_timestamp);
    }

private:
    FrameId m_frameId;
    StreamId m_streamId;
    Timestamp m_timestamp;
};

// ==================== 时间戳帧元数据实现 ====================
class TimestampFrameMetadata : public IFrameMetadata {
public:
    TimestampFrameMetadata(FrameId fid, StreamId sid, Timestamp ts)
        : m_frameId(fid), m_streamId(sid), m_timestamp(ts) {}

    FrameId frameId() const override { return m_frameId; }
    StreamId streamId() const override { return m_streamId; }
    Timestamp timestamp() const override { return m_timestamp; }

    bool shouldSyncWith(const IFrameMetadata& other, std::chrono::microseconds tolerance) const override {
        // 容差对齐：两者的物理时间差在设定阈值内
        auto diff = std::chrono::duration_cast<std::chrono::microseconds>(m_timestamp - other.timestamp());
        return std::abs(diff.count()) <= tolerance.count();
    }

    int compareTo(const IFrameMetadata& other) const override {
        if (m_timestamp < other.timestamp()) return -1;
        if (m_timestamp > other.timestamp()) return 1;
        return 0;
    }

    std::unique_ptr<IFrameMetadata> clone() const override {
        return std::make_unique<TimestampFrameMetadata>(m_frameId, m_streamId, m_timestamp);
    }

private:
    FrameId m_frameId;
    StreamId m_streamId;
    Timestamp m_timestamp;
};
```

手搓这一模块时，你要深深理解：`IFrameMetadata` 不是冷冰冰的数字，它是整个实时系统的“时间刻度”。有了它，数据的流动才真正拥有了“时序美感”，多路流对齐才可能建立坚固的物理秩序。
