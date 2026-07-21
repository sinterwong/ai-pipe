# 13. Join 超时降级与看门狗机制 (Join Watchdog)

在多流实时管线中，我们通过对齐引擎保证了数据的时序铁律。然而，真实的物理世界充满了变数：
一个摄像头可能由于线缆松动突然物理断线，或者其推流帧率骤降。
*   **卡死灾难**：如果 Join 合流节点有一路输入端口突然完全没有了数据，根据对齐策略，Join 节点将永远无法凑齐数据。
*   **级联故障**：因为 Join 节点卡死、无法消费，组内其他还在正常推流的分支队列会在背压作用下瞬间被全部丢弃，甚至由于多输入重调度终止导致整个管线陷入永久性的“半瘫痪（Dead Lock-like Hanging）”状态。

AI Pipe 手搓了一套轻量级的 **看门狗超时降级（Join Watchdog）** 机制，实现了无数据输入时的自动唤醒与业务自愈。

---

## 1. 核心设计原理

### 1.1 看门狗定时重调度与扫描
在 AI Pipe 引擎内部，重调度通常是由“新数据推入事件（Data Event）”驱动的。
如果其中一路分支完全没有新数据流入，执行引擎中将不会产生任何关于该 Join 节点的调度任务（Event-driven silence）。

**解决方案**：
引入**主动扫描（Active Watchdog Thread）**：
```
+------------------------------------------------------------+
|                       Watchdog Thread                      |
|                                                            |
|         扫描周期 = join_wait_timeout / 4                     |
|                                                            |
+------------------------------------------------------------+
                               | (Periodic Scan)
                               v
                  Join Node (Partially Ready)
                               |
                   (Wait Time > Timeout ?)
                               |
                       +-------+-------+
                       |               |
                    (Yes)             (No)
                       |               |
                       v               v
                Trigger Degrade     Do Nothing
```

1.  **轻量常驻线程**：当管线处于 `STREAM` 运行状态且 `join_wait_timeout > 0` 时，引擎内部会唤醒一个轻量的常驻 `Watchdog` 线程。
2.  **节拍式扫描（Heartbeat Scan）**：
    为了平衡扫描开销与超时精度，看门狗以 **`join_wait_timeout / 4`** 作为扫描节拍。如果超时上限设为 100ms，扫描线程每 25ms 醒来一次。
3.  **活跃度追溯**：
    看门狗扫描所有多输入（Join）节点的 `NodeState`：
    *   如果节点处于**部分就绪（Partially Ready）**状态（即有的输入队列不空，有的队列为空，节点因此无法被 `StreamScheduler` 正常重调度激活）。
    *   且该状态持续时间已超过 `join_wait_timeout` 上限。
    *   **动作**：看门狗线程主动构建一个调度任务，将该 Join 节点提交回线程池执行，强行打破死锁。

---

## 2. 核心巧思与实现细节

### 2.1 降级执行策略（Timeout Degraded Policies）
当 Join 节点由于超时被看门狗强行唤醒调度时，数据明显是不齐的。此时引擎必须支持两种降级手段以应对不同的业务诉求：

| 降级策略 | 引擎具体行为 | 适用场景 |
|---|---|---|
| **部分输入执行（`PartialInputs`）** | 将当前已就绪的端口数据 Pop 出来打包，**缺失的端口在 `PortDataMap` 中直接不出现**。强行调用 `node->process`。 | **高宽容合流**。节点本身具备容错能力，缺少一路数据也能运行（例如多相机目标追踪，缺一个相机画面仍能预测大致轨迹）。 |
| **跳过当前帧（`SkipFrame`）** | **直接对当前卡住的已就绪队头进行 tryPop 弹出并清除丢弃**。通过 callback 汇报 reason 为 `join wait timeout`。节点不执行。 | **硬性精确合流**。不接受任何残缺数据，超时只希望能把卡住的流推开、继续运行下一帧。 |

### 2.2 守护线程的优雅生命周期控制
看门狗是一个独立的物理 OS 线程。如果处理不当，会导致整个进程在退出时卡死（Cannot join thread）或引发析构后的野指针崩溃。

**实现巧思**：
*   **随引擎起停**：看门狗线程必须在 `startStreaming` 中才被拉起，并在 `stopStreaming` 第一瞬间被宣告退出（`m_watchdogStop = true`）。
*   **唤醒等待平衡**：在常态无超时时，看门狗不应占用任何 CPU 算力。采用 `std::condition_variable` 的 `wait_for` 等待退出标记，一旦收到退出广播，看门狗能在 1ms 内瞬间响应，安全析构归还线程，实现极致的优雅。

---

## 3. 手搓实现参考骨架

你可以根据以下完美融合进引擎内部的 `JoinWatchdog` 骨架进行手搓复习：

```cpp
enum class JoinTimeoutPolicy {
    PartialInputs,
    SkipFrame
};

class ExecutionEngine::Impl {
public:
    void startWatchdog(std::chrono::milliseconds timeout, JoinTimeoutPolicy policy) {
        m_joinTimeout = timeout;
        m_timeoutPolicy = policy;
        m_watchdogStop.store(false);

        m_watchdogThread = std::thread([this]() {
            auto interval = m_joinTimeout / 4;
            if (interval.count() == 0) interval = std::chrono::milliseconds(5);

            while (!m_watchdogStop.load(std::memory_order_relaxed)) {
                std::unique_lock<std::mutex> lock(m_watchdogMutex);
                m_watchdogCond.wait_for(lock, interval, [this]() {
                    return m_watchdogStop.load();
                });

                if (m_watchdogStop.load()) break;

                // 执行周期性扫描
                scanJoinNodesForTimeout();
            }
        });
    }

    void stopWatchdog() {
        m_watchdogStop.store(true);
        m_watchdogCond.notify_all();
        if (m_watchdogThread.joinable()) {
            m_watchdogThread.join();
        }
    }

private:
    void scanJoinNodesForTimeout() {
        auto now = std::chrono::steady_clock::now();
        for (const auto& pair : m_nodeStates) {
            auto state = pair.second;
            if (state->node->getExpectedInputPorts().size() <= 1) {
                continue; // 过滤非合流（Join）节点
            }

            // 判断是否处于部分就绪状态
            size_t emptyCount = 0;
            size_t readyCount = 0;
            for (const auto& port : state->node->getExpectedInputPorts()) {
                if (state->inputQueues.at(port)->size() == 0) {
                    emptyCount++;
                } else {
                    readyCount++;
                }
            }

            // 只有当部分就绪、且没有活跃并发任务执行它时，才可能卡死，需要评估超时
            if (readyCount > 0 && emptyCount > 0 && state->activeTasks.load() == 0) {
                if (now - state->lastActiveTime > m_joinTimeout) {
                    // 超时条件达成！触发降级唤醒
                    triggerDegradedExecution(state);
                }
            }
        }
    }

    void triggerDegradedExecution(std::shared_ptr<NodeState> state) {
        // 增加活跃计数，防止看门狗与线程池并发调度发生交叉感染
        state->activeTasks.fetch_add(1);

        m_threadPool->submit([this, state]() {
            PortDataMap inputs;

            if (m_timeoutPolicy == JoinTimeoutPolicy::PartialInputs) {
                // 部分输入降级：能拉多少拉多少
                for (const auto& port : state->node->getExpectedInputPorts()) {
                    std::shared_ptr<DataPacket> pkt;
                    if (state->inputQueues.at(port)->tryPop(pkt)) {
                        inputs[port] = pkt;
                    }
                }

                // 执行 process
                PortDataMap outputs;
                state->node->process(inputs, outputs);
                propagateOutputs(state, outputs);

            } else if (m_timeoutPolicy == JoinTimeoutPolicy::SkipFrame) {
                // 跳过帧降级：强行清空当前的落后队头，促使后续对齐
                for (const auto& port : state->node->getExpectedInputPorts()) {
                    std::shared_ptr<DataPacket> pkt;
                    if (state->inputQueues.at(port)->tryPop(pkt)) {
                        // 触发超时丢弃回调
                        reportTimeoutDrop(state->node->getName(), pkt->id);
                    }
                }
            }

            state->lastActiveTime = std::chrono::steady_clock::now();
            state->activeTasks.fetch_sub(1);
        });
    }

    std::chrono::milliseconds m_joinTimeout{0};
    JoinTimeoutPolicy m_timeoutPolicy{JoinTimeoutPolicy::PartialInputs};

    std::thread m_watchdogThread;
    std::mutex m_watchdogMutex;
    std::condition_variable m_watchdogCond;
    std::atomic<bool> m_watchdogStop{false};
};
```

手搓 `JoinWatchdog` 期间，你将真正理解流式管道框架在“高可靠性、高可用性自愈设计”层面的终极追求。有了它，管线不再是脆弱一推就倒的实验室玩具，而是具备在极端丢包、断线物理场景下，100% 保持稳定运转的工业级容灾神兵。
