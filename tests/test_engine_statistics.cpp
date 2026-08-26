#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/execution_types.hpp"
#include "ai_pipe/graph.hpp"
#include "helper_nodes.hpp"
#include <cmath>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace ai_pipe;
using namespace std::chrono_literals;

// LatencyHistogram Tests

class LatencyHistogramTest : public ::testing::Test {
protected:
  LatencyHistogram m_histogram;
};

TEST_F(LatencyHistogramTest, InitialState) {
  EXPECT_EQ(m_histogram.totalCount(), 0);
  for (std::size_t i = 0; i < LatencyHistogram::k_num_buckets; ++i) {
    EXPECT_EQ(m_histogram.buckets[i].load(), 0);
  }
}

TEST_F(LatencyHistogramTest, BucketIndexCalculation) {
  // First bucket: <10us
  EXPECT_EQ(LatencyHistogram::getBucketIndex(0), 0);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(5), 0);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(9), 0);

  // Second bucket: <25us
  EXPECT_EQ(LatencyHistogram::getBucketIndex(10), 1);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(24), 1);

  // Third bucket: <50us
  EXPECT_EQ(LatencyHistogram::getBucketIndex(25), 2);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(49), 2);

  // Fourth bucket: <100us
  EXPECT_EQ(LatencyHistogram::getBucketIndex(50), 3);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(99), 3);

  // Fifth bucket: <250us
  EXPECT_EQ(LatencyHistogram::getBucketIndex(100), 4);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(249), 4);

  // Sixth bucket: <500us
  EXPECT_EQ(LatencyHistogram::getBucketIndex(250), 5);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(499), 5);

  // Seventh bucket: <1ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(500), 6);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(999), 6);

  // Eighth bucket: <2.5ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(1000), 7);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(2499), 7);

  // Ninth bucket: <5ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(2500), 8);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(4999), 8);

  // Tenth bucket: <10ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(5000), 9);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(9999), 9);

  // Eleventh bucket: <25ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(10000), 10);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(24999), 10);

  // Twelfth bucket: <50ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(25000), 11);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(49999), 11);

  // Thirteenth bucket: <100ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(50000), 12);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(99999), 12);

  // Fourteenth bucket: <250ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(100000), 13);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(249999), 13);

  // Fifteenth bucket: <500ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(250000), 14);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(499999), 14);

  // Last bucket: >=500ms
  EXPECT_EQ(LatencyHistogram::getBucketIndex(500000), 15);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(1000000), 15);
  EXPECT_EQ(LatencyHistogram::getBucketIndex(UINT64_MAX), 15);
}

TEST_F(LatencyHistogramTest, RecordLatency) {
  m_histogram.record(5);   // bucket 0
  m_histogram.record(15);  // bucket 1
  m_histogram.record(30);  // bucket 2
  m_histogram.record(75);  // bucket 3
  m_histogram.record(150); // bucket 4

  EXPECT_EQ(m_histogram.buckets[0].load(), 1);
  EXPECT_EQ(m_histogram.buckets[1].load(), 1);
  EXPECT_EQ(m_histogram.buckets[2].load(), 1);
  EXPECT_EQ(m_histogram.buckets[3].load(), 1);
  EXPECT_EQ(m_histogram.buckets[4].load(), 1);
  EXPECT_EQ(m_histogram.totalCount(), 5);
}

TEST_F(LatencyHistogramTest, RecordMultipleSameSamples) {
  for (int i = 0; i < 100; ++i) {
    m_histogram.record(50); // bucket 3
  }

  EXPECT_EQ(m_histogram.buckets[3].load(), 100);
  EXPECT_EQ(m_histogram.totalCount(), 100);
}

TEST_F(LatencyHistogramTest, Reset) {
  m_histogram.record(5);
  m_histogram.record(100);
  m_histogram.record(10000);

  EXPECT_EQ(m_histogram.totalCount(), 3);

  m_histogram.reset();

  EXPECT_EQ(m_histogram.totalCount(), 0);
  for (std::size_t i = 0; i < LatencyHistogram::k_num_buckets; ++i) {
    EXPECT_EQ(m_histogram.buckets[i].load(), 0);
  }
}

TEST_F(LatencyHistogramTest, BucketLabels) {
  EXPECT_EQ(LatencyHistogram::getBucketLabel(0), "<10us");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(1), "<25us");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(2), "<50us");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(3), "<100us");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(4), "<250us");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(5), "<500us");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(6), "<1ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(7), "<2.5ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(8), "<5ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(9), "<10ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(10), "<25ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(11), "<50ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(12), "<100ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(13), "<250ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(14), "<500ms");
  EXPECT_EQ(LatencyHistogram::getBucketLabel(15), ">=500ms");
}

TEST_F(LatencyHistogramTest, ConcurrentRecording) {
  const int num_threads = 8;
  const int records_per_thread = 1000;

  std::vector<std::thread> threads;
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, records_per_thread]() {
      for (int i = 0; i < records_per_thread; ++i) {
        m_histogram.record(t * 100 + i);
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  EXPECT_EQ(m_histogram.totalCount(), num_threads * records_per_thread);
}

// NodeStatistics Tests

class NodeStatisticsTest : public ::testing::Test {
protected:
  NodeStatistics m_stats{"test_node"};
};

TEST_F(NodeStatisticsTest, DefaultConstruction) {
  NodeStatistics default_stats;
  EXPECT_TRUE(default_stats.node_name.empty());
  EXPECT_EQ(default_stats.execution_count, 0);
  EXPECT_EQ(default_stats.success_count, 0);
  EXPECT_EQ(default_stats.failure_count, 0);
  EXPECT_EQ(default_stats.total_processing_us, 0);
  EXPECT_EQ(default_stats.max_processing_us, 0);
  EXPECT_EQ(default_stats.total_input_count, 0);
  EXPECT_EQ(default_stats.total_output_count, 0);
  EXPECT_EQ(default_stats.current_queue_depth, 0);
}

TEST_F(NodeStatisticsTest, NamedConstruction) {
  EXPECT_EQ(m_stats.node_name, "test_node");
}

TEST_F(NodeStatisticsTest, AvgProcessingTimeUs_NoExecutions) {
  EXPECT_DOUBLE_EQ(m_stats.avgProcessingTimeUs(), 0.0);
}

TEST_F(NodeStatisticsTest, AvgProcessingTimeUs_WithExecutions) {
  m_stats.success_count = 10;
  m_stats.total_processing_us = 5000;

  EXPECT_DOUBLE_EQ(m_stats.avgProcessingTimeUs(), 500.0);
}

TEST_F(NodeStatisticsTest, SuccessRate_NoExecutions) {
  EXPECT_DOUBLE_EQ(m_stats.successRate(), 100.0);
}

TEST_F(NodeStatisticsTest, SuccessRate_AllSuccess) {
  m_stats.execution_count = 100;
  m_stats.success_count = 100;

  EXPECT_DOUBLE_EQ(m_stats.successRate(), 100.0);
}

TEST_F(NodeStatisticsTest, SuccessRate_SomeFailures) {
  m_stats.execution_count = 100;
  m_stats.success_count = 80;
  m_stats.failure_count = 20;

  EXPECT_DOUBLE_EQ(m_stats.successRate(), 80.0);
}

TEST_F(NodeStatisticsTest, SuccessRate_AllFailures) {
  m_stats.execution_count = 100;
  m_stats.success_count = 0;
  m_stats.failure_count = 100;

  EXPECT_DOUBLE_EQ(m_stats.successRate(), 0.0);
}

// AtomicNodeStatistics Tests

class AtomicNodeStatisticsTest : public ::testing::Test {
protected:
  AtomicNodeStatistics m_atomicStats;
};

// Initial State

TEST_F(AtomicNodeStatisticsTest, InitialState) {
  EXPECT_EQ(m_atomicStats.execution_count.load(), 0);
  EXPECT_EQ(m_atomicStats.success_count.load(), 0);
  EXPECT_EQ(m_atomicStats.failure_count.load(), 0);
  EXPECT_EQ(m_atomicStats.total_processing_us.load(), 0);
  EXPECT_EQ(m_atomicStats.min_processing_us.load(),
            std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(m_atomicStats.max_processing_us.load(), 0);
}

// Raw Atomic Increments (still valid for direct field access)

TEST_F(AtomicNodeStatisticsTest, AtomicIncrements) {
  m_atomicStats.execution_count.fetch_add(1);
  m_atomicStats.success_count.fetch_add(1);
  m_atomicStats.total_processing_us.fetch_add(1000);

  EXPECT_EQ(m_atomicStats.execution_count.load(), 1);
  EXPECT_EQ(m_atomicStats.success_count.load(), 1);
  EXPECT_EQ(m_atomicStats.total_processing_us.load(), 1000);
}

// recordExecution

TEST_F(AtomicNodeStatisticsTest, RecordExecution_FirstValue) {
  m_atomicStats.recordExecution(true, 500);

  EXPECT_EQ(m_atomicStats.execution_count.load(), 1);
  EXPECT_EQ(m_atomicStats.success_count.load(), 1);
  EXPECT_EQ(m_atomicStats.failure_count.load(), 0);
  EXPECT_EQ(m_atomicStats.total_processing_us.load(), 500);
  EXPECT_EQ(m_atomicStats.min_processing_us.load(), 500);
  EXPECT_EQ(m_atomicStats.max_processing_us.load(), 500);
}

TEST_F(AtomicNodeStatisticsTest, RecordExecution_Failure) {
  m_atomicStats.recordExecution(false, 200);

  EXPECT_EQ(m_atomicStats.execution_count.load(), 1);
  EXPECT_EQ(m_atomicStats.success_count.load(), 0);
  EXPECT_EQ(m_atomicStats.failure_count.load(), 1);
  EXPECT_EQ(m_atomicStats.total_processing_us.load(), 200);
}

TEST_F(AtomicNodeStatisticsTest, RecordExecution_NewMin) {
  m_atomicStats.recordExecution(true, 500);
  m_atomicStats.recordExecution(true, 100);

  EXPECT_EQ(m_atomicStats.min_processing_us.load(), 100);
  EXPECT_EQ(m_atomicStats.max_processing_us.load(), 500);
}

TEST_F(AtomicNodeStatisticsTest, RecordExecution_NewMax) {
  m_atomicStats.recordExecution(true, 500);
  m_atomicStats.recordExecution(true, 1000);

  EXPECT_EQ(m_atomicStats.min_processing_us.load(), 500);
  EXPECT_EQ(m_atomicStats.max_processing_us.load(), 1000);
}

TEST_F(AtomicNodeStatisticsTest, RecordExecution_NoChange) {
  m_atomicStats.recordExecution(true, 100);
  m_atomicStats.recordExecution(true, 1000);
  m_atomicStats.recordExecution(true, 500); // Between min and max

  EXPECT_EQ(m_atomicStats.min_processing_us.load(), 100);
  EXPECT_EQ(m_atomicStats.max_processing_us.load(), 1000);
}

TEST_F(AtomicNodeStatisticsTest, RecordExecution_MixedSuccessFailure) {
  m_atomicStats.recordExecution(true, 100);
  m_atomicStats.recordExecution(false, 200);
  m_atomicStats.recordExecution(true, 300);
  m_atomicStats.recordExecution(false, 50);

  EXPECT_EQ(m_atomicStats.execution_count.load(), 4);
  EXPECT_EQ(m_atomicStats.success_count.load(), 2);
  EXPECT_EQ(m_atomicStats.failure_count.load(), 2);
  EXPECT_EQ(m_atomicStats.total_processing_us.load(), 650); // 100+200+300+50
  EXPECT_EQ(m_atomicStats.min_processing_us.load(), 50);
  EXPECT_EQ(m_atomicStats.max_processing_us.load(), 300);
}

// Reset

TEST_F(AtomicNodeStatisticsTest, Reset) {
  m_atomicStats.recordExecution(true, 100);
  m_atomicStats.recordExecution(true, 1000);
  m_atomicStats.recordExecution(false, 500);

  m_atomicStats.reset();

  EXPECT_EQ(m_atomicStats.execution_count.load(), 0);
  EXPECT_EQ(m_atomicStats.success_count.load(), 0);
  EXPECT_EQ(m_atomicStats.failure_count.load(), 0);
  EXPECT_EQ(m_atomicStats.total_processing_us.load(), 0);
  EXPECT_EQ(m_atomicStats.min_processing_us.load(),
            std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(m_atomicStats.max_processing_us.load(), 0);
}

// Snapshot

TEST_F(AtomicNodeStatisticsTest, Snapshot) {
  m_atomicStats.execution_count.store(10);
  m_atomicStats.success_count.store(8);
  m_atomicStats.failure_count.store(2);
  m_atomicStats.total_processing_us.store(8000);
  m_atomicStats.recordExecution(true, 100);  // updates min to 100
  m_atomicStats.recordExecution(true, 2000); // updates max to 2000

  auto snapshot = m_atomicStats.snapshot("my_node");

  EXPECT_EQ(snapshot.node_name, "my_node");
  // execution_count = 10 (stored) + 2 (from recordExecution) = 12
  EXPECT_EQ(snapshot.execution_count, 12);
  // success_count = 8 (stored) + 2 (from recordExecution) = 10
  EXPECT_EQ(snapshot.success_count, 10);
  EXPECT_EQ(snapshot.failure_count, 2);
  // total_processing_us = 8000 + 100 + 2000 = 10100
  EXPECT_EQ(snapshot.total_processing_us, 10100);
  EXPECT_EQ(snapshot.min_processing_us, 100);
  EXPECT_EQ(snapshot.max_processing_us, 2000);
}

TEST_F(AtomicNodeStatisticsTest, Snapshot_UninitializedMin) {
  auto snapshot = m_atomicStats.snapshot("node");
  EXPECT_EQ(snapshot.min_processing_us,
            std::numeric_limits<std::uint64_t>::max());
}

// Concurrent Updates

TEST_F(AtomicNodeStatisticsTest, ConcurrentUpdates) {
  const int num_threads = 8;
  const int updates_per_thread = 1000;

  std::vector<std::thread> threads;
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, updates_per_thread]() {
      for (int i = 0; i < updates_per_thread; ++i) {
        m_atomicStats.recordExecution(true, static_cast<std::uint64_t>(i + 1));
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  EXPECT_EQ(m_atomicStats.execution_count.load(),
            num_threads * updates_per_thread);
  EXPECT_EQ(m_atomicStats.success_count.load(),
            num_threads * updates_per_thread);
  EXPECT_EQ(m_atomicStats.min_processing_us.load(), 1);
  EXPECT_EQ(m_atomicStats.max_processing_us.load(), updates_per_thread);
}

// EngineStatistics Tests

class EngineStatisticsTest : public ::testing::Test {
protected:
  EngineStatistics m_stats;
};

TEST_F(EngineStatisticsTest, InitialState) {
  EXPECT_EQ(m_stats.total_executions.load(), 0);
  EXPECT_EQ(m_stats.successful_executions.load(), 0);
  EXPECT_EQ(m_stats.failed_executions.load(), 0);
  EXPECT_EQ(m_stats.total_input_frames.load(), 0);
  EXPECT_EQ(m_stats.total_output_frames.load(), 0);
  EXPECT_EQ(m_stats.total_dropped_frames.load(), 0);
  EXPECT_EQ(m_stats.total_queue_pushes.load(), 0);
  EXPECT_EQ(m_stats.total_queue_pops.load(), 0);
  EXPECT_EQ(m_stats.queue_full_events.load(), 0);
  EXPECT_EQ(m_stats.total_processing_time_us.load(), 0);
  EXPECT_EQ(m_stats.total_wait_time_us.load(), 0);
  EXPECT_EQ(m_stats.total_schedule_time_us.load(), 0);
  EXPECT_EQ(m_stats.latency_histogram.totalCount(), 0);
}

TEST_F(EngineStatisticsTest, AtomicIncrements) {
  m_stats.total_executions.fetch_add(10);
  m_stats.successful_executions.fetch_add(8);
  m_stats.failed_executions.fetch_add(2);
  m_stats.total_input_frames.fetch_add(100);
  m_stats.total_output_frames.fetch_add(80);
  m_stats.total_dropped_frames.fetch_add(20);

  EXPECT_EQ(m_stats.total_executions.load(), 10);
  EXPECT_EQ(m_stats.successful_executions.load(), 8);
  EXPECT_EQ(m_stats.failed_executions.load(), 2);
  EXPECT_EQ(m_stats.total_input_frames.load(), 100);
  EXPECT_EQ(m_stats.total_output_frames.load(), 80);
  EXPECT_EQ(m_stats.total_dropped_frames.load(), 20);
}

TEST_F(EngineStatisticsTest, RecordLatency) {
  m_stats.recordLatency(50);
  m_stats.recordLatency(500);
  m_stats.recordLatency(5000);

  EXPECT_EQ(m_stats.latency_histogram.totalCount(), 3);
}

TEST_F(EngineStatisticsTest, SuccessRate_NoExecutions) {
  EXPECT_DOUBLE_EQ(m_stats.successRate(), 100.0);
}

TEST_F(EngineStatisticsTest, SuccessRate_AllSuccess) {
  m_stats.total_executions.store(100);
  m_stats.successful_executions.store(100);

  EXPECT_DOUBLE_EQ(m_stats.successRate(), 100.0);
}

TEST_F(EngineStatisticsTest, SuccessRate_Mixed) {
  m_stats.total_executions.store(100);
  m_stats.successful_executions.store(75);

  EXPECT_DOUBLE_EQ(m_stats.successRate(), 75.0);
}

TEST_F(EngineStatisticsTest, DropRate_NoFrames) {
  EXPECT_DOUBLE_EQ(m_stats.dropRate(), 0.0);
}

TEST_F(EngineStatisticsTest, DropRate_NoDrops) {
  m_stats.total_output_frames.store(100);
  m_stats.total_dropped_frames.store(0);

  EXPECT_DOUBLE_EQ(m_stats.dropRate(), 0.0);
}

TEST_F(EngineStatisticsTest, DropRate_SomeDrops) {
  m_stats.total_output_frames.store(80);
  m_stats.total_dropped_frames.store(20);

  EXPECT_DOUBLE_EQ(m_stats.dropRate(), 20.0); // 20 / (80+20) * 100
}

TEST_F(EngineStatisticsTest, DropRate_AllDropped) {
  m_stats.total_output_frames.store(0);
  m_stats.total_dropped_frames.store(100);

  EXPECT_DOUBLE_EQ(m_stats.dropRate(), 100.0);
}

TEST_F(EngineStatisticsTest, InputOutputRatio_NoOutput) {
  m_stats.total_input_frames.store(100);
  m_stats.total_output_frames.store(0);

  EXPECT_DOUBLE_EQ(m_stats.inputOutputRatio(), 0.0);
}

TEST_F(EngineStatisticsTest, InputOutputRatio_Equal) {
  m_stats.total_input_frames.store(100);
  m_stats.total_output_frames.store(100);

  EXPECT_DOUBLE_EQ(m_stats.inputOutputRatio(), 1.0);
}

TEST_F(EngineStatisticsTest, InputOutputRatio_MoreInputs) {
  m_stats.total_input_frames.store(200);
  m_stats.total_output_frames.store(100);

  EXPECT_DOUBLE_EQ(m_stats.inputOutputRatio(), 2.0);
}

TEST_F(EngineStatisticsTest, AvgProcessingTimeUs_NoExecutions) {
  EXPECT_DOUBLE_EQ(m_stats.avgProcessingTimeUs(), 0.0);
}

TEST_F(EngineStatisticsTest, AvgProcessingTimeUs_WithExecutions) {
  m_stats.successful_executions.store(10);
  m_stats.total_processing_time_us.store(10000);

  EXPECT_DOUBLE_EQ(m_stats.avgProcessingTimeUs(), 1000.0);
}

TEST_F(EngineStatisticsTest, AvgWaitTimeUs_NoPops) {
  EXPECT_DOUBLE_EQ(m_stats.avgWaitTimeUs(), 0.0);
}

TEST_F(EngineStatisticsTest, AvgWaitTimeUs_WithPops) {
  m_stats.total_queue_pops.store(100);
  m_stats.total_wait_time_us.store(50000);

  EXPECT_DOUBLE_EQ(m_stats.avgWaitTimeUs(), 500.0);
}

TEST_F(EngineStatisticsTest, AvgScheduleTimeUs_NoExecutions) {
  EXPECT_DOUBLE_EQ(m_stats.avgScheduleTimeUs(), 0.0);
}

TEST_F(EngineStatisticsTest, AvgScheduleTimeUs_WithExecutions) {
  m_stats.total_executions.store(50);
  m_stats.total_schedule_time_us.store(25000);

  EXPECT_DOUBLE_EQ(m_stats.avgScheduleTimeUs(), 500.0);
}

TEST_F(EngineStatisticsTest, BackwardCompatibility_TotalFramesProcessed) {
  m_stats.total_output_frames.store(123);
  EXPECT_EQ(m_stats.totalFramesProcessed(), 123);
}

TEST_F(EngineStatisticsTest, BackwardCompatibility_TotalFramesDropped) {
  m_stats.total_dropped_frames.store(456);
  EXPECT_EQ(m_stats.totalFramesDropped(), 456);
}

TEST_F(EngineStatisticsTest, Reset) {
  m_stats.total_executions.store(100);
  m_stats.successful_executions.store(90);
  m_stats.failed_executions.store(10);
  m_stats.total_input_frames.store(200);
  m_stats.total_output_frames.store(180);
  m_stats.total_dropped_frames.store(20);
  m_stats.total_queue_pushes.store(500);
  m_stats.total_queue_pops.store(400);
  m_stats.queue_full_events.store(5);
  m_stats.total_processing_time_us.store(100000);
  m_stats.total_wait_time_us.store(50000);
  m_stats.total_schedule_time_us.store(10000);
  m_stats.recordLatency(100);
  m_stats.recordLatency(1000);

  m_stats.reset();

  EXPECT_EQ(m_stats.total_executions.load(), 0);
  EXPECT_EQ(m_stats.successful_executions.load(), 0);
  EXPECT_EQ(m_stats.failed_executions.load(), 0);
  EXPECT_EQ(m_stats.total_input_frames.load(), 0);
  EXPECT_EQ(m_stats.total_output_frames.load(), 0);
  EXPECT_EQ(m_stats.total_dropped_frames.load(), 0);
  EXPECT_EQ(m_stats.total_queue_pushes.load(), 0);
  EXPECT_EQ(m_stats.total_queue_pops.load(), 0);
  EXPECT_EQ(m_stats.queue_full_events.load(), 0);
  EXPECT_EQ(m_stats.total_processing_time_us.load(), 0);
  EXPECT_EQ(m_stats.total_wait_time_us.load(), 0);
  EXPECT_EQ(m_stats.total_schedule_time_us.load(), 0);
  EXPECT_EQ(m_stats.latency_histogram.totalCount(), 0);
}

TEST_F(EngineStatisticsTest, ConcurrentUpdates) {
  const int num_threads = 8;
  const int updates_per_thread = 1000;

  std::vector<std::thread> threads;
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, updates_per_thread]() {
      for (int i = 0; i < updates_per_thread; ++i) {
        m_stats.total_executions.fetch_add(1);
        m_stats.successful_executions.fetch_add(1);
        m_stats.total_input_frames.fetch_add(1);
        m_stats.total_output_frames.fetch_add(1);
        m_stats.total_queue_pushes.fetch_add(1);
        m_stats.total_queue_pops.fetch_add(1);
        m_stats.recordLatency(i * 10);
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  EXPECT_EQ(m_stats.total_executions.load(), num_threads * updates_per_thread);
  EXPECT_EQ(m_stats.successful_executions.load(),
            num_threads * updates_per_thread);
  EXPECT_EQ(m_stats.total_input_frames.load(),
            num_threads * updates_per_thread);
  EXPECT_EQ(m_stats.total_output_frames.load(),
            num_threads * updates_per_thread);
  EXPECT_EQ(m_stats.latency_histogram.totalCount(),
            num_threads * updates_per_thread);
}

// EngineStatisticsSnapshot Tests

class EngineStatisticsSnapshotTest : public ::testing::Test {
protected:
  EngineStatistics m_stats;

  void SetUp() override {
    m_stats.total_executions.store(100);
    m_stats.successful_executions.store(90);
    m_stats.failed_executions.store(10);
    m_stats.total_input_frames.store(50);
    m_stats.total_output_frames.store(45);
    m_stats.total_dropped_frames.store(5);
    m_stats.total_queue_pushes.store(200);
    m_stats.total_queue_pops.store(200);
    m_stats.queue_full_events.store(10);
    m_stats.total_processing_time_us.store(900000);
    m_stats.total_wait_time_us.store(100000);
    m_stats.total_schedule_time_us.store(50000);
  }
};

TEST_F(EngineStatisticsSnapshotTest, DefaultConstruction) {
  EngineStatisticsSnapshot snapshot;

  EXPECT_EQ(snapshot.total_executions, 0);
  EXPECT_EQ(snapshot.successful_executions, 0);
  EXPECT_EQ(snapshot.failed_executions, 0);
  EXPECT_EQ(snapshot.total_input_frames, 0);
  EXPECT_EQ(snapshot.total_output_frames, 0);
  EXPECT_EQ(snapshot.total_dropped_frames, 0);
  EXPECT_TRUE(snapshot.node_stats.empty());
}

TEST_F(EngineStatisticsSnapshotTest, ConstructionFromStats) {
  EngineStatisticsSnapshot snapshot(m_stats);

  EXPECT_EQ(snapshot.total_executions, 100);
  EXPECT_EQ(snapshot.successful_executions, 90);
  EXPECT_EQ(snapshot.failed_executions, 10);
  EXPECT_EQ(snapshot.total_input_frames, 50);
  EXPECT_EQ(snapshot.total_output_frames, 45);
  EXPECT_EQ(snapshot.total_dropped_frames, 5);
  EXPECT_EQ(snapshot.total_queue_pushes, 200);
  EXPECT_EQ(snapshot.total_queue_pops, 200);
  EXPECT_EQ(snapshot.queue_full_events, 10);
  EXPECT_EQ(snapshot.total_processing_time_us, 900000);
  EXPECT_EQ(snapshot.total_wait_time_us, 100000);
  EXPECT_EQ(snapshot.total_schedule_time_us, 50000);
}

TEST_F(EngineStatisticsSnapshotTest, ComputedRates) {
  EngineStatisticsSnapshot snapshot(m_stats);

  EXPECT_DOUBLE_EQ(snapshot.success_rate, 90.0);
  EXPECT_DOUBLE_EQ(snapshot.drop_rate, 10.0); // 5 / (45+5) * 100
}

TEST_F(EngineStatisticsSnapshotTest, BackwardCompatibility) {
  EngineStatisticsSnapshot snapshot(m_stats);

  EXPECT_EQ(snapshot.totalFramesProcessed(), 45);
  EXPECT_EQ(snapshot.totalFramesDropped(), 5);
}

TEST_F(EngineStatisticsSnapshotTest, InputOutputRatio) {
  EngineStatisticsSnapshot snapshot(m_stats);

  double expected = 50.0 / 45.0;
  EXPECT_NEAR(snapshot.inputOutputRatio(), expected, 0.001);
}

TEST_F(EngineStatisticsSnapshotTest, AvgProcessingTimeUs) {
  EngineStatisticsSnapshot snapshot(m_stats);

  EXPECT_DOUBLE_EQ(snapshot.avgProcessingTimeUs(), 10000.0); // 900000 / 90
}

TEST_F(EngineStatisticsSnapshotTest, AvgWaitTimeUs) {
  EngineStatisticsSnapshot snapshot(m_stats);

  EXPECT_DOUBLE_EQ(snapshot.avgWaitTimeUs(), 500.0); // 100000 / 200
}

TEST_F(EngineStatisticsSnapshotTest, AvgScheduleTimeUs) {
  EngineStatisticsSnapshot snapshot(m_stats);

  EXPECT_DOUBLE_EQ(snapshot.avgScheduleTimeUs(), 500.0); // 50000 / 100
}

TEST_F(EngineStatisticsSnapshotTest, ElapsedSeconds) {
  EngineStatisticsSnapshot snapshot(m_stats);

  // Should be a small positive value (time between stats creation and snapshot)
  EXPECT_GE(snapshot.elapsedSeconds(), 0.0);
}

TEST_F(EngineStatisticsSnapshotTest, HistogramData) {
  // Record some latencies
  m_stats.recordLatency(5);    // bucket 0: <10us
  m_stats.recordLatency(100);  // bucket 4: <250us
  m_stats.recordLatency(5000); // bucket 9: <10ms

  EngineStatisticsSnapshot snapshot(m_stats);
  auto hist_data = snapshot.histogramData();

  EXPECT_EQ(hist_data.size(), LatencyHistogram::k_num_buckets);
  EXPECT_EQ(hist_data[0].first, "<10us");
  EXPECT_EQ(hist_data[0].second, 1);
  EXPECT_EQ(hist_data[4].second, 1);
  EXPECT_EQ(hist_data[9].second, 1);
}

TEST_F(EngineStatisticsSnapshotTest, LatencyPercentiles_Empty) {
  EngineStatisticsSnapshot snapshot(m_stats);
  auto percentiles = snapshot.latencyPercentiles();

  EXPECT_EQ(percentiles.size(), 5);
  EXPECT_EQ(percentiles[0].first, "p50");
  EXPECT_DOUBLE_EQ(percentiles[0].second, 0.0);
  EXPECT_EQ(percentiles[1].first, "p90");
  EXPECT_EQ(percentiles[2].first, "p95");
  EXPECT_EQ(percentiles[3].first, "p99");
  EXPECT_EQ(percentiles[4].first, "p99.9");
}

TEST_F(EngineStatisticsSnapshotTest, LatencyPercentiles_WithData) {
  // Record latencies to create a distribution
  // 50 samples at <100us (bucket 3), 40 samples at <1ms (bucket 6), 10 samples
  // at <10ms (bucket 9)
  for (int i = 0; i < 50; ++i) {
    m_stats.recordLatency(75); // bucket 3 (<100us)
  }
  for (int i = 0; i < 40; ++i) {
    m_stats.recordLatency(750); // bucket 6 (<1ms)
  }
  for (int i = 0; i < 10; ++i) {
    m_stats.recordLatency(7500); // bucket 9 (<10ms)
  }

  EngineStatisticsSnapshot snapshot(m_stats);
  auto percentiles = snapshot.latencyPercentiles();

  EXPECT_EQ(percentiles.size(), 5);
  // p50 should be in bucket 3 (<100us), returns upper bound 100
  EXPECT_DOUBLE_EQ(percentiles[0].second, 100.0);
  // p90 should be in bucket 6 (<1ms), returns upper bound 1000
  EXPECT_DOUBLE_EQ(percentiles[1].second, 1000.0);
}

TEST_F(EngineStatisticsSnapshotTest, HistogramCopiedCorrectly) {
  for (std::size_t i = 0; i < LatencyHistogram::k_num_buckets; ++i) {
    m_stats.latency_histogram.buckets[i].store(i + 1);
  }

  EngineStatisticsSnapshot snapshot(m_stats);

  for (std::size_t i = 0; i < LatencyHistogram::k_num_buckets; ++i) {
    EXPECT_EQ(snapshot.latency_histogram_buckets[i], i + 1);
  }
}

TEST_F(EngineStatisticsSnapshotTest, NodeStatsInitiallyEmpty) {
  EngineStatisticsSnapshot snapshot(m_stats);
  EXPECT_TRUE(snapshot.node_stats.empty());
}

TEST_F(EngineStatisticsSnapshotTest, NodeStatsCanBePopulated) {
  EngineStatisticsSnapshot snapshot(m_stats);

  NodeStatistics node1("node1");
  node1.execution_count = 10;
  node1.success_count = 9;

  NodeStatistics node2("node2");
  node2.execution_count = 20;
  node2.success_count = 18;

  snapshot.node_stats.push_back(node1);
  snapshot.node_stats.push_back(node2);

  EXPECT_EQ(snapshot.node_stats.size(), 2);
  EXPECT_EQ(snapshot.node_stats[0].node_name, "node1");
  EXPECT_EQ(snapshot.node_stats[1].node_name, "node2");
}

// Edge Cases and Boundary Tests

class EngineStatisticsEdgeCasesTest : public ::testing::Test {
protected:
  EngineStatistics m_stats;
};

TEST_F(EngineStatisticsEdgeCasesTest, VeryLargeValues) {
  m_stats.total_executions.store(UINT64_MAX - 1);
  m_stats.successful_executions.store(UINT64_MAX - 1);

  EXPECT_EQ(m_stats.total_executions.load(), UINT64_MAX - 1);
  EXPECT_NEAR(m_stats.successRate(), 100.0, 0.001);
}

TEST_F(EngineStatisticsEdgeCasesTest, OverflowProtection) {
  m_stats.total_executions.store(UINT64_MAX);

  // This would overflow, but atomic fetch_add handles it
  m_stats.total_executions.fetch_add(1);

  EXPECT_EQ(m_stats.total_executions.load(), 0); // Wraps around
}

TEST_F(EngineStatisticsEdgeCasesTest, ZeroDivisionProtection) {
  // All computations with zero denominators should return safe values
  EXPECT_DOUBLE_EQ(m_stats.successRate(), 100.0);
  EXPECT_DOUBLE_EQ(m_stats.dropRate(), 0.0);
  EXPECT_DOUBLE_EQ(m_stats.throughput(), 0.0);
  EXPECT_DOUBLE_EQ(m_stats.inputOutputRatio(), 0.0);
  EXPECT_DOUBLE_EQ(m_stats.avgProcessingTimeUs(), 0.0);
  EXPECT_DOUBLE_EQ(m_stats.avgWaitTimeUs(), 0.0);
  EXPECT_DOUBLE_EQ(m_stats.avgScheduleTimeUs(), 0.0);
}

TEST_F(EngineStatisticsEdgeCasesTest, LatencyHistogramBoundaries) {
  m_stats.recordLatency(10);  // Should go to bucket 1, not 0
  m_stats.recordLatency(25);  // Should go to bucket 2, not 1
  m_stats.recordLatency(50);  // Should go to bucket 3, not 2
  m_stats.recordLatency(100); // Should go to bucket 4, not 3

  EXPECT_EQ(m_stats.latency_histogram.buckets[0].load(), 0);
  EXPECT_EQ(m_stats.latency_histogram.buckets[1].load(), 1);
  EXPECT_EQ(m_stats.latency_histogram.buckets[2].load(), 1);
  EXPECT_EQ(m_stats.latency_histogram.buckets[3].load(), 1);
  EXPECT_EQ(m_stats.latency_histogram.buckets[4].load(), 1);
}

TEST_F(EngineStatisticsEdgeCasesTest, MaxLatencyValue) {
  m_stats.recordLatency(UINT64_MAX);

  // Should go to the last bucket
  EXPECT_EQ(m_stats.latency_histogram.buckets[15].load(), 1);
}

// Integration Tests

class EngineStatisticsIntegrationTest : public ::testing::Test {
protected:
  EngineStatistics m_stats;
};

TEST_F(EngineStatisticsIntegrationTest, SimulateWorkload) {
  const int num_frames = 1000;
  const int drop_interval = 20;    // Every 20th frame is dropped
  const int failure_interval = 50; // Every 50th frame fails

  int dropped = 0;
  int failed = 0;
  int succeeded = 0;

  for (int i = 0; i < num_frames; ++i) {
    m_stats.total_input_frames.fetch_add(1);
    m_stats.total_executions.fetch_add(1);

    bool is_failure = (i % failure_interval == 0);
    bool is_dropped = (i % drop_interval == 0);

    if (is_failure) {
      m_stats.failed_executions.fetch_add(1);
      ++failed;
    } else {
      m_stats.successful_executions.fetch_add(1);
      ++succeeded;

      if (is_dropped) {
        m_stats.total_dropped_frames.fetch_add(1);
        ++dropped;
      } else {
        m_stats.total_output_frames.fetch_add(1);
      }

      m_stats.recordLatency(100 + (i % 1000));
    }

    m_stats.total_queue_pushes.fetch_add(1);
    m_stats.total_queue_pops.fetch_add(1);
    m_stats.total_processing_time_us.fetch_add(1000);
  }

  EngineStatisticsSnapshot snapshot(m_stats);

  EXPECT_EQ(snapshot.total_input_frames, num_frames);
  EXPECT_EQ(snapshot.total_executions, num_frames);
  EXPECT_EQ(snapshot.successful_executions, succeeded);
  EXPECT_EQ(snapshot.failed_executions, failed);
  EXPECT_EQ(snapshot.total_dropped_frames, dropped);

  EXPECT_GT(snapshot.success_rate, 95.0); // ~98% success
  EXPECT_LT(snapshot.success_rate, 100.0);
  EXPECT_GT(snapshot.drop_rate, 0.0);
  EXPECT_LT(snapshot.drop_rate, 10.0);

  auto hist_data = snapshot.histogramData();
  std::uint64_t total_hist = 0;
  for (const auto &[label, count] : hist_data) {
    total_hist += count;
  }
  EXPECT_GT(total_hist, 0);
}

TEST_F(EngineStatisticsIntegrationTest, ResetAndReuse) {
  // First workload
  m_stats.total_executions.store(100);
  m_stats.successful_executions.store(100);
  m_stats.total_output_frames.store(100);

  EXPECT_EQ(m_stats.total_executions.load(), 100);

  // Reset
  m_stats.reset();

  EXPECT_EQ(m_stats.total_executions.load(), 0);

  // Second workload
  m_stats.total_executions.store(50);
  m_stats.successful_executions.store(45);

  EXPECT_EQ(m_stats.total_executions.load(), 50);
  EXPECT_DOUBLE_EQ(m_stats.successRate(), 90.0);
}

// Live wiring integration: every snapshot field must be fed
// by a real engine run, not just exist in the API.

namespace {

ai_pipe::PortDataPtr makeStatsFrame() { return std::make_shared<PortData>(); }

} // namespace

class StatisticsWiringTest : public ::testing::Test {
protected:
  void buildLinear(Graph &graph, std::chrono::milliseconds delay) {
    graph.addNode(
        std::make_shared<ai_pipe_unit_test::PassThroughNode>("source"));
    graph.addNode(
        std::make_shared<ai_pipe_unit_test::PassThroughNode>("mid", delay));
    graph.addNode(std::make_shared<ai_pipe_unit_test::SinkNode>("sink"));
    graph.addEdge("source", "output", "mid", "input");
    graph.addEdge("mid", "output", "sink", "input");
  }
};

TEST_F(StatisticsWiringTest, BatchRunFeedsAllCounters) {
  Graph graph;
  buildLinear(graph, 2ms); // guarantees measurable end-to-end latency

  auto engine = createBatchEngine(2);
  ASSERT_TRUE(engine->initialize(&graph).isOk());

  PortDataMap inputs;
  inputs["source"] = makeStatsFrame();
  ASSERT_TRUE(engine->execute(inputs, true).isOk());

  auto stats = engine->statistics();

  EXPECT_EQ(stats.total_executions, 3u);
  EXPECT_EQ(stats.successful_executions, 3u);
  EXPECT_EQ(stats.failed_executions, 0u);
  EXPECT_EQ(stats.total_input_frames, 1u);
  EXPECT_EQ(stats.total_output_frames, 1u);
  EXPECT_GT(stats.total_queue_pops, 0u);
  EXPECT_GT(stats.total_processing_time_us, 0u);
  EXPECT_GT(stats.total_wait_time_us, 0u)
      << "dequeue frame-age accounting must be live";

  // End-to-end latency histogram: the 2ms mid node forces >= 2000us
  std::uint64_t histogram_total = 0;
  for (auto count : stats.latency_histogram_buckets) {
    histogram_total += count;
  }
  EXPECT_EQ(histogram_total, 1u) << "one frame reached the sink";
  auto percentiles = stats.latencyPercentiles();
  ASSERT_FALSE(percentiles.empty());
  EXPECT_GT(percentiles[0].second, 0.0) << "p50 must be non-zero";

  // Per-node statistics populated
  ASSERT_EQ(stats.node_stats.size(), 3u);
  for (const auto &node : stats.node_stats) {
    EXPECT_EQ(node.execution_count, 1u) << node.node_name;
    EXPECT_EQ(node.success_count, 1u) << node.node_name;
    EXPECT_EQ(node.current_queue_depth, 0u) << node.node_name;
  }
}

TEST_F(StatisticsWiringTest, DisabledStatisticsStayZero) {
  Graph graph;
  buildLinear(graph, 2ms);

  EngineConfig config = EngineConfig::batch(2);
  config.enable_statistics = false;
  auto engine = ExecutionEngine::create(config);
  ASSERT_TRUE(engine->initialize(&graph).isOk());

  PortDataMap inputs;
  inputs["source"] = makeStatsFrame();
  ASSERT_TRUE(engine->execute(inputs, true).isOk());

  auto stats = engine->statistics();

  EXPECT_EQ(stats.total_queue_pops, 0u);
  EXPECT_EQ(stats.total_wait_time_us, 0u);
  EXPECT_EQ(stats.total_schedule_time_us, 0u);
  std::uint64_t histogram_total = 0;
  for (auto count : stats.latency_histogram_buckets) {
    histogram_total += count;
  }
  EXPECT_EQ(histogram_total, 0u);
  for (const auto &node : stats.node_stats) {
    EXPECT_EQ(node.execution_count, 0u) << node.node_name;
  }
}

TEST_F(StatisticsWiringTest, StreamingCountsInputFrames) {
  Graph graph;
  buildLinear(graph, 0ms);

  auto engine = createStreamEngine(2, 32);
  ASSERT_TRUE(engine->initialize(&graph).isOk());
  ASSERT_TRUE(engine->startStreaming().isOk());

  constexpr int k_frames = 10;
  for (int i = 0; i < k_frames; ++i) {
    ASSERT_TRUE(engine->pushInput("source", makeStatsFrame()).isOk());
  }
  ASSERT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  engine->stopStreaming(false);

  auto stats = engine->statistics();
  EXPECT_EQ(stats.total_input_frames, static_cast<std::uint64_t>(k_frames));
  EXPECT_EQ(stats.total_output_frames, static_cast<std::uint64_t>(k_frames));
  EXPECT_GE(stats.total_queue_pops, static_cast<std::uint64_t>(k_frames));
  EXPECT_EQ(stats.total_executions, static_cast<std::uint64_t>(k_frames * 3));
}
