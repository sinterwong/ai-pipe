# 03. Lock-Free Bounded MPMC 队列 (LockFreeNodeQueue)

在实时 AI 管道中，数据在节点间的流转具有极高的频率（如每秒数百帧的高清图像），传统的基于 `std::mutex` 和 `std::condition_variable` 的阻塞队列会在高并发场景下产生显著的线程上下文切换开销，使微秒级的超低延迟目标沦为空谈。

为此，AI Pipe 的底层数据通信网络彻底抛弃了重量级锁，采用 **Dmitry Vyukov 的有界 MPMC 队列算法**，并进行了定制开发，完美支持了**背压管理（KeepLatest/DropHead 驱逐策略）**、**非占有式 Peek（互斥边界控制）**以及 **Cache-Line 对齐**。

---

## 1. 核心设计原理

### 1.1 Dmitry Vyukov Bounded MPMC 算法精髓
该算法之所以能够做到 Lock-Free，是因为它将对队列的并发竞争分解为**对单个 Slot 的原子状态流转竞争**。每个槽位（Slot）都带有一个单调递增的“序列号（Sequence Tag）”。

队列的底层物理结构是一个固定大小的环形数组：
```cpp
struct Cell {
    std::atomic<size_t> sequence;
    PortDataPtr data;
};
```

*   **初始状态**：对于容量为 `N` 的队列，第 `i` 个槽位的 `sequence` 初始值被赋予其物理下标 `i`。
*   **Push 规则**：
    1.  生产者原子地获取当前的写指针 `pos = m_enqueuePos.load(relaxed)`。
    2.  定位到对应的 Slot：`cell = &m_buffer[pos & m_bufferMask]`。
    3.  读取槽位的序列号：`seq = cell->sequence.load(acquire)`。
    4.  **状态校验**：
        *   若 `seq == pos`，说明槽位处于**空闲可写**状态。生产者尝试利用 `compare_exchange_weak` 将 `m_enqueuePos` 推进到 `pos + 1`。若成功，抢占该 Slot，写入数据，并利用 `release` 语义将 `cell->sequence` 设为 `pos + 1`。
        *   若 `seq < pos`，说明 Slot 中仍有数据未被 Pop 出来（队列满）。
        *   若 `seq > pos`，说明其他生产者已经抢占了该 Slot 正在写入或已写完。生产者需要重试。
*   **Pop 规则**：
    1.  消费者原子地获取当前的读指针 `pos = m_dequeuePos.load(relaxed)`。
    2.  定位到对应的 Slot：`cell = &m_buffer[pos & m_bufferMask]`。
    3.  读取槽位的序列号：`seq = cell->sequence.load(acquire)`。
    4.  **状态校验**：
        *   若 `seq == pos + 1`，说明槽位处于**数据已写入可读**状态。消费者尝试将 `m_dequeuePos` 推进到 `pos + 1`。若成功，抢占 Slot，取出数据，并利用 `release` 语义将 `cell->sequence` 设为 `pos + m_bufferMask + 1`（代表 Slot 变为空闲，等待下一个循环写）。
        *   若 `seq < pos + 1`，说明 Slot 中数据还未写入（队列空）。
        *   若 `seq > pos + 1`，说明其他消费者已经抢走了数据。消费者需要重试。

---

## 2. 核心巧思与实现细节

### 2.1 Cache-Line 伪共享防护（False Sharing Mitigation）
在多核处理器中，不同核心的缓存行通常是 64 字节（k_cache_line_size）。如果写指针 `m_enqueuePos`、读指针 `m_dequeuePos` 以及相邻的 `Cell` 被分配在同一个缓存行中，由于不同核心高频写入，会导致缓存一致性协议（如 MESI）频繁将其他核心的缓存行置为 Invalid，进而引发巨大的硬件延迟开销。

**实现巧思**：
对关键成员变量以及 `Cell` 数据结构实施 `alignas(k_cache_line_size)` 物理对齐，确保读写指针分布在完全隔离的 Cache-Line 上：
```cpp
struct alignas(64) Cell {
    std::atomic<size_t> sequence;
    PortDataPtr data;
};

class LockFreeNodeQueue {
    // ...
    alignas(64) std::atomic<size_t> m_enqueuePos{0};
    alignas(64) std::atomic<size_t> m_dequeuePos{0};
    alignas(64) Cell* m_buffer{nullptr};
    size_t m_bufferMask{0};
    // ...
};
```

### 2.2 宽松内存序（Relaxed Memory Order）与同步点设计
不加思索地对所有的原子操作使用默认的 `std::memory_order_seq_cst`（顺序一致性）会强制插入硬件层面的内存屏障（Barrier），降低执行效率。

**实现巧思**：
*   **非同步路径**：在尝试 CAS 之前，使用 `std::memory_order_relaxed` 加载 `m_enqueuePos` / `m_dequeuePos`。因为这一步仅仅是为了确定当前的猜测位置，其最终的安全性是由 CAS 操作和 Slot 的 `sequence` 校验来锁定的。
*   **槽位同步路径**：
    *   在 Push 成功抢占槽位后，写入 `cell->data`。必须使用 `std::memory_order_release` 写入序列号 `cell->sequence`，确保此前对数据包内容的写入对于其他核心“可见”。
    *   在 Pop 时，加载序列号 `cell->sequence` 必须使用 `std::memory_order_acquire`，以此与生产者的 Release 形成**同步点（Synchronizes-with）**，确保安全地消费到最新的 `cell->data` 指针。

### 2.3 "Lock-Free" 的精确边界：HeadMutex 互斥控制
既然是 Lock-Free MPMC，为什么在实现中会有一把 `m_headMutex` 互斥锁？

**设计背景**：
在多路对齐或多输入 Join 调度中，引擎必须支持 **非占有式读（TryPeek）**。Peek 意味着“在不修改 `m_dequeuePos` 的前提下，窥探队头的数据是否符合对齐条件”。
如果在并发下，一个消费者在进行 `TryPeek`（读取当前的队头槽位），而多个生产者因为队列满，触发了 `DropHead` 或 `KeepLatest` 驱逐策略。驱逐需要调用 `evictOne` 强行将队头槽位清空。
*   **竞争风险**：非占有式 Peek 读取的 Slot 与驱逐操作强行清空/覆盖的 Slot 是同一个。这会造成严重的读写竞争，甚至出现 ABA 问题。
*   **解决方案**：引入一把轻量级的 `m_headMutex`。
    *   **CAS 消费路径（TryPop）保持完全无锁**：因为 `tryPop` 本身是通过原子 CAS 推进 `m_dequeuePos` 竞争槽位所有权的，它不与 `evictOne` 发生数据破坏竞态（Pop-vs-Pop 安全）。
    *   **Peek 路径与驱逐路径（evictOne）上锁互斥**：在 `tryPeek` 和生产者侧强行驱逐（`evictOne` 推进 `m_dequeuePos`）时，必须获取 `m_headMutex`。这样将读-驱逐竞争进行了安全的物理串行化，保持了核心 Pop 路径的无锁超凡性能。

### 2.4 KeepLatest 并发自愈驱逐语义
`KeepLatest` 指：当队列达到容量上限 `N` 时，允许新元素写入，并自动驱逐最老的元素，使队列深度始终保持在 `N` 左右。

**实现巧思**（并发边界与自愈）：
在多生产者并发下，`KeepLatest` 的“驱逐并写入”操作不可能是原子的。
当 `P` 个并发生产者同时写入已满的队列时，它们会并发调用 `evictOne`（获取 `m_headMutex` 并弹出最老槽位，推进 `m_dequeuePos`），随后各自入队。
*   **临时涨库现象**：在竞争的窗口期，队列的大小可能短暂达到 `N + P - 1`，但决不会超过环形缓冲的物理容量（通常物理大小是 `N` 的两倍或按 2 的幂取整）。
*   **自愈特性**：每次 Push 在写入前都会自旋检测 `size() >= N`，如果是，就会主动驱逐最老帧直至 `size() < N` 之后再行入队。任何后续的无竞争写入，都会瞬间让队列深度回落并保持在限制的 `N` 以内。
*   **用户侧硬性约束**：如果用户业务要求任何时刻、任何物理瞬间队列内包数量**严格不准超过** `N`，必须在外部自行将生产者进行串行化保护（这也是本组件作为微内核底层，提供给上层调用方的明确契约）。

---

## 3. 手搓实现参考骨架

你可以根据以下极其详尽的骨架来实现：

```cpp
template <typename T>
class LockFreeNodeQueue {
public:
    struct alignas(64) Cell {
        std::atomic<size_t> sequence;
        std::shared_ptr<const T> data;
    };

    explicit LockFreeNodeQueue(size_t capacity) {
        // 容量向上对齐到 2 的幂
        size_t realCapacity = 1;
        while (realCapacity < capacity) realCapacity <<= 1;
        m_bufferMask = realCapacity - 1;
        m_buffer = new Cell[realCapacity];
        for (size_t i = 0; i < realCapacity; ++i) {
            m_buffer[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~LockFreeNodeQueue() { delete[] m_buffer; }

    bool tryPush(std::shared_ptr<const T> data) {
        size_t pos = m_enqueuePos.load(std::memory_order_relaxed);
        while (true) {
            Cell* cell = &m_buffer[pos & m_bufferMask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (diff == 0) {
                if (m_enqueuePos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    cell->data = std::move(data);
                    cell->sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                // 队列已满
                return false;
            } else {
                pos = m_enqueuePos.load(std::memory_order_relaxed);
            }
        }
    }

    bool tryPop(std::shared_ptr<const T>& data) {
        size_t pos = m_dequeuePos.load(std::memory_order_relaxed);
        while (true) {
            Cell* cell = &m_buffer[pos & m_bufferMask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
            if (diff == 0) {
                if (m_dequeuePos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    data = std::move(cell->data);
                    cell->sequence.store(pos + m_bufferMask + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                // 队列已空
                return false;
            } else {
                pos = m_dequeuePos.load(std::memory_order_relaxed);
            }
        }
    }

    // TryPeek 必须配合 m_headMutex
    bool tryPeek(std::shared_ptr<const T>& data) {
        std::lock_guard<std::mutex> lock(m_headMutex);
        size_t pos = m_dequeuePos.load(std::memory_order_relaxed);
        Cell* cell = &m_buffer[pos & m_bufferMask];
        size_t seq = cell->sequence.load(std::memory_order_acquire);
        if (seq == pos + 1) {
            data = cell->data; // 仅仅拷贝指针，不动读指针
            return true;
        }
        return false;
    }

private:
    alignas(64) std::atomic<size_t> m_enqueuePos{0};
    alignas(64) std::atomic<size_t> m_dequeuePos{0};
    alignas(64) Cell* m_buffer{nullptr};
    size_t m_bufferMask{0};
    std::mutex m_headMutex; // 仅保护 Peek 与 强行驱逐（evictOne）
};
```

通过这一无锁高性能结构，AI Pipe 彻底打通了数据传输的微秒通路。在手搓时，请务必保证你理解了 `Cell` 的 `sequence` 的变化逻辑：它是防止并发交叉污染的最强大哨兵。
