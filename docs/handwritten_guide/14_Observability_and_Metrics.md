# 14. 可观测性与性能指标 (LatencyHistogram)

一个只注重运行速度、却无法洞察自身执行状态（如当前 FPS、P99 端到端延迟、节点积压数）的管道引擎，在工业生产环境下就是个无法维护的“黑盒”。

然而，如果频繁收集性能指标，使用重锁机制（Mutex）会瞬间把高并发的多核心线程池性能拉低 10 倍以上。AI Pipe 运用了极为硬核的 **无锁原子直方图（Wait-Free Atomically Latency Histogram）**，实现了零性能损耗、毫秒级高精度指标观测。

---

## 1. 核心设计原理

### 1.1 16 桶位延迟直方图设计（16-Bucket Spec）
为了在极低耗时的微秒级到高耗时的毫秒级之间，全面、不失真地记录每一次执行的端到端延迟，AI Pipe 设计并固化了 **16 个物理桶位（Buckets）**：

```
Bucket  0: <10us     Bucket  1: <25us     Bucket  2: <50us     Bucket  3: <100us
Bucket  4: <250us    Bucket  5: <500us    Bucket  6: <1ms      Bucket  7: <2.5ms
Bucket  8: <5ms      Bucket  9: <10ms     Bucket 10: <25ms     Bucket 11: <50ms
Bucket 12: <100ms    Bucket 13: <250ms    Bucket 14: <500ms    Bucket 15: >=500ms
```

*   **物理实现**：直方图底层只是一个包含 16 个元素的原子数组：
    `std::atomic<uint64_t> m_buckets[16];`
*   **Wait-Free 写入**：
    当一个包在 Sink 节点完成处理时，引擎计算其端到端物理耗时 $T$。
    1.  通过级联判断（或二分查找），判定 $T$ 属于哪一个桶位下标 $i$。
    2.  调用 `m_buckets[i].fetch_add(1, std::memory_order_relaxed)`。
    *   **至高美学**：这一步完全不需要任何锁保护（甚至不需要 CAS 自旋），它是纯粹的 **Wait-Free（无等待）** 硬件原子加。在 x86 架构下，它被编译为单条带锁前缀指令（如 `LOCK XADD`），耗时不超过 5ns，对热路径执行毫无影响。

---

## 2. 核心巧思与实现细节

### 2.1 高吞吐分位数估算算法（P50 - P99.9 Percentiles）
有了 16 个桶位的累加计数，我们如何无锁地估算出常见的 P50、P90、P99 延迟分位数？

**实现巧思（线性插值算法）**：
1.  **快照抓取**：首先原子读取（`load`）这 16 个桶位的当前值，累加得到当前的总样本数 $N$。
2.  **确定目标样本序号**：对于 P99，目标样本序号 $target = N \times 0.99$。
3.  **区间定位**：从 Bucket 0 开始累加计数。当累加值跨越 $target$ 时，说明 P99 延迟必然落在当前桶位区间 $[L_i, H_i]$ 中。
4.  **区间内线性插值（Linear Interpolation）**：
    为了让估算值更逼真，而不是死板地返回区间的中间值，我们采用线性插值：
    $$PercentileValue = L_i + \frac{target - CountBefore}{CountInBucket} \times (H_i - L_i)$$
    通过该算法，可以在 O(1) 的超凡速度下，估算出极其逼真、精确的 P99/P99.9 百分位数，完全不需要将百万级别的历史样本存储在内存中做重量级排序。

### 2.2 性能监控零消耗开关（Zero-Overhead Toggle）
即使 Wait-Free 开销极低，但在极限跑分场景下，100% 的极致主义者依然希望连这 5ns 的开销也一并省下。

**实现巧思**：
在 `EngineConfig` 中支持 `enable_statistics` 静态/动态双重开关。
在引擎核心路径上：
```cpp
if (m_config.enable_statistics) {
    recordLatency(elapsed);
}
```
当该配置设为 `false` 时，分支预测器（Branch Predictor）会判定其为常假（Likely False），在硬件执行流水线中，该分支被彻底跳过，甚至不消耗 CPU 的取指译码窗口，实现了真正意义上的 **Zero-Overhead 零损耗空转**。

---

## 3. 手搓实现参考骨架

你可以根据以下高度优化的 Wait-Free `LatencyHistogram` 骨架进行手搓：

```cpp
class LatencyHistogram {
public:
    LatencyHistogram() {
        for (auto& bucket : m_buckets) {
            bucket.store(0, std::memory_order_relaxed);
        }
    }

    // Wait-Free 物理记录
    void record(uint64_t elapsedUs) {
        size_t idx = getBucketIndex(elapsedUs);
        m_buckets[idx].fetch_add(1, std::memory_order_relaxed);
    }

    // 抓取并计算分位数
    std::unordered_map<std::string, double> getPercentiles() const {
        uint64_t snap[16];
        uint64_t total = 0;
        for (size_t i = 0; i < 16; ++i) {
            snap[i] = m_buckets[i].load(std::memory_order_relaxed);
            total += snap[i];
        }

        std::unordered_map<std::string, double> res;
        if (total == 0) return {{"p50", 0}, {"p90", 0}, {"p95", 0}, {"p99", 0}};

        res["p50"] = estimatePercentile(snap, total, 0.50);
        res["p90"] = estimatePercentile(snap, total, 0.90);
        res["p95"] = estimatePercentile(snap, total, 0.95);
        res["p99"] = estimatePercentile(snap, total, 0.99);
        return res;
    }

private:
    static size_t getBucketIndex(uint64_t us) {
        if (us < 10) return 0;
        if (us < 25) return 1;
        if (us < 50) return 2;
        if (us < 100) return 3;
        if (us < 250) return 4;
        if (us < 500) return 5;
        if (us < 1000) return 6;    // 1ms
        if (us < 2500) return 7;
        if (us < 5000) return 8;
        if (us < 10000) return 9;   // 10ms
        if (us < 25000) return 10;
        if (us < 50000) return 11;
        if (us < 100000) return 12; // 100ms
        if (us < 250000) return 13;
        if (us < 500000) return 14;
        return 15;                  // >= 500ms
    }

    static std::pair<double, double> getBucketBounds(size_t idx) {
        static constexpr double bounds[17] = {
            0, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000,
            10000, 25000, 50000, 100000, 250000, 500000, 1000000
        };
        return {bounds[idx], bounds[idx+1]};
    }

    double estimatePercentile(const uint64_t snap[16], uint64_t total, double pct) const {
        double target = total * pct;
        uint64_t cumulative = 0;

        for (size_t i = 0; i < 16; ++i) {
            if (cumulative + snap[i] >= target) {
                auto [low, high] = getBucketBounds(i);
                double countInBucket = snap[i];
                if (countInBucket == 0) return low;

                double needed = target - cumulative;
                // 线性插值估算
                return low + (needed / countInBucket) * (high - low);
            }
            cumulative += snap[i];
        }
        return 500000.0;
    }

    alignas(64) std::atomic<uint64_t> m_buckets[16];
};
```

手搓这个模块时，你将彻底被“Wait-Free（无等待）”的硬件执行美感征服。它是微秒级实时并发系统中，唯一能兼顾“极深可观测性”与“极致物理吞吐量”的银色子弹。
