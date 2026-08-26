#include "ai_pipe/ai_pipe.hpp"
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <mutex>
#include <string>
#include <vector>

namespace ai_pipe {
namespace {

using namespace std::chrono_literals;

constexpr auto k_wait = 5000ms;

// Forwards its input; records every frame id it sees.
class PassNode : public ILogicNode {
public:
  explicit PassNode(std::string name) : ILogicNode(std::move(name)) {}

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    auto it = inputs.find("input");
    if (it == inputs.end() || !it->second) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_seen.push_back(it->second->frameId());
    }
    outputs["output"] = it->second;
  }

  std::vector<FrameId> seen() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_seen;
  }

  bool flushed() const { return m_flushed.load(); }

  void onEndOfStream(PortDataMap &, std::shared_ptr<PipelineContext>) override {
    m_flushed.store(true);
  }

private:
  mutable std::mutex m_mutex;
  std::vector<FrameId> m_seen;
  std::atomic<bool> m_flushed{false};
};

// Accumulates inputs and only emits once it has a full batch - the
// canonical node that strands data at the end of a finite stream unless
// a flush hook exists.
class BatchingNode : public ILogicNode {
public:
  BatchingNode(std::string name, std::size_t batch_size)
      : ILogicNode(std::move(name)), m_batchSize(batch_size) {}

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    auto it = inputs.find("input");
    if (it == inputs.end() || !it->second) {
      return;
    }
    m_pending.push_back(it->second);
    if (m_pending.size() < m_batchSize) {
      return;
    }
    outputs["output"] = emitBatch();
  }

  void onEndOfStream(PortDataMap &outputs,
                     std::shared_ptr<PipelineContext>) override {
    m_flushCalls.fetch_add(1);
    if (m_pending.empty()) {
      return;
    }
    outputs["output"] = emitBatch(); // The residue that would be stranded
  }

  int flushCalls() const { return m_flushCalls.load(); }

private:
  PortDataPtr emitBatch() {
    auto packet = std::make_shared<PortData>();
    packet->id = m_pending.back()->id;
    packet->setParam<std::int64_t>("batch_size",
                                   static_cast<std::int64_t>(m_pending.size()));
    m_pending.clear();
    return packet;
  }

  std::size_t m_batchSize;
  std::vector<PortDataPtr> m_pending; // Touched only under the engine's
                                      // process/flush mutual exclusion
  std::atomic<int> m_flushCalls{0};
};

// Terminal node; counts what arrives and whether EOS reached it.
class SinkNode : public ILogicNode {
public:
  explicit SinkNode(std::string name) : ILogicNode(std::move(name)) {}

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  void process(const PortDataMap &inputs, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {
    auto it = inputs.find("input");
    if (it == inputs.end() || !it->second) {
      return;
    }
    m_received.fetch_add(1);
    const auto batch = it->second->param<std::int64_t>("batch_size");
    if (batch) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_batches.push_back(batch.value());
    }
  }

  void onEndOfStream(PortDataMap &, std::shared_ptr<PipelineContext>) override {
    m_sawEos.store(true);
  }

  int received() const { return m_received.load(); }
  bool sawEos() const { return m_sawEos.load(); }
  std::vector<std::int64_t> batches() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_batches;
  }

private:
  std::atomic<int> m_received{0};
  std::atomic<bool> m_sawEos{false};
  mutable std::mutex m_mutex;
  std::vector<std::int64_t> m_batches;
};

// Two-input join.
class JoinNode : public ILogicNode {
public:
  explicit JoinNode(std::string name) : ILogicNode(std::move(name)) {}

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"left", "right"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    m_executions.fetch_add(1);
    auto packet = std::make_shared<PortData>();
    for (const auto &[port, data] : inputs) {
      if (data) {
        packet->id = data->id;
      }
    }
    outputs["output"] = packet;
  }

  void onEndOfStream(PortDataMap &, std::shared_ptr<PipelineContext>) override {
    m_flushed.store(true);
  }

  int executions() const { return m_executions.load(); }
  bool flushed() const { return m_flushed.load(); }

private:
  std::atomic<int> m_executions{0};
  std::atomic<bool> m_flushed{false};
};

// A flush hook that throws; must not strand the rest of the graph.
class ThrowingFlushNode : public ILogicNode {
public:
  explicit ThrowingFlushNode(std::string name) : ILogicNode(std::move(name)) {}

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    auto it = inputs.find("input");
    if (it != inputs.end()) {
      outputs["output"] = it->second;
    }
  }

  void onEndOfStream(PortDataMap &, std::shared_ptr<PipelineContext>) override {
    throw std::runtime_error("flush hook failure");
  }
};

PortDataPtr makePacket() { return std::make_shared<PortData>(); }

// A packet with an explicit frame id.
//
// Needed whenever two branches must pair at a join: the engine's
// ingress stamps ids from one counter per stream, so two packets pushed
// into different ports of the same stream get *different* ids and would
// never align. Real multi-branch sources carry a shared capture id;
// these tests say so explicitly.
PortDataPtr makePacket(FrameId id) {
  auto packet = std::make_shared<PortData>();
  packet->id = id;
  return packet;
}

// source -> pass -> sink, streaming.
struct LinearFixture {
  std::shared_ptr<PassNode> source = std::make_shared<PassNode>("source");
  std::shared_ptr<PassNode> middle = std::make_shared<PassNode>("middle");
  std::shared_ptr<SinkNode> sink = std::make_shared<SinkNode>("sink");
  Pipeline pipeline;

  Result<void> build(PipelineOptions options = PipelineOptions::stream()) {
    Graph graph;
    graph.addNode(source);
    graph.addNode(middle);
    graph.addNode(sink);
    (void)graph.addEdge("source", "output", "middle", "input");
    (void)graph.addEdge("middle", "output", "sink", "input");

    auto built = Pipeline::create()
                     .withGraph(std::move(graph))
                     .withOptions(options)
                     .build();
    if (!built) {
      return Result<void>::err(built.error());
    }
    pipeline = std::move(built.value());
    return pipeline.start();
  }
};

// Linear propagation

TEST(EndOfStreamTest, ReachesSinkAndUnblocksWaiter) {
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(fx.pipeline.pushInput("source", makePacket()));
  }
  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));

  const auto waited = fx.pipeline.waitForEndOfStream(k_wait);
  ASSERT_TRUE(waited) << waited.error().toString();

  EXPECT_TRUE(fx.pipeline.isEndOfStreamReached());
  EXPECT_TRUE(fx.sink->sawEos());
  EXPECT_TRUE(fx.middle->flushed());
  EXPECT_EQ(fx.sink->received(), 5);

  fx.pipeline.stop();
}

TEST(EndOfStreamTest, QueuedDataIsProcessedBeforeEos) {
  // The ordering guarantee that makes the latch behave as if in band:
  // every packet pushed before the signal must reach the sink first.
  // Queues are sized past the burst so nothing is dropped for capacity
  // reasons and a shortfall can only mean EOS overtook data.
  LinearFixture fx;
  auto options = PipelineOptions::stream();
  options.queue_capacity = 512;
  ASSERT_TRUE(fx.build(options));

  constexpr int k_frames = 200;
  for (int i = 0; i < k_frames; ++i) {
    ASSERT_TRUE(fx.pipeline.pushInput("source", makePacket()));
  }
  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));
  ASSERT_TRUE(fx.pipeline.waitForEndOfStream(k_wait));

  EXPECT_EQ(fx.sink->received(), k_frames);
  fx.pipeline.stop();
}

TEST(EndOfStreamTest, EosOnAnAlreadyIdlePipelineStillPropagates) {
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  ASSERT_TRUE(fx.pipeline.pushInput("source", makePacket()));
  ASSERT_TRUE(fx.pipeline.waitForDrain(0, k_wait));

  // Queues are empty when the signal lands: EOS must settle immediately
  // rather than wait for a data event that will never come.
  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));
  ASSERT_TRUE(fx.pipeline.waitForEndOfStream(k_wait));
  EXPECT_TRUE(fx.sink->sawEos());

  fx.pipeline.stop();
}

TEST(EndOfStreamTest, PipelineStaysRunningAfterEos) {
  // EOS says "no more data", not "shut down": the caller still owns the
  // decision to stop, and statistics must remain readable.
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  ASSERT_TRUE(fx.pipeline.pushInput("source", makePacket()));
  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));
  ASSERT_TRUE(fx.pipeline.waitForEndOfStream(k_wait));

  EXPECT_TRUE(fx.pipeline.isStreaming());
  EXPECT_GE(fx.pipeline.statistics().total_executions, 1u);

  fx.pipeline.stop();
}

// Flush hook

TEST(EndOfStreamTest, FlushHookEmitsStrandedResidue) {
  auto source = std::make_shared<PassNode>("source");
  auto batcher = std::make_shared<BatchingNode>("batcher", 4);
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(batcher);
  graph.addNode(sink);
  (void)graph.addEdge("source", "output", "batcher", "input");
  (void)graph.addEdge("batcher", "output", "sink", "input");

  auto built = Pipeline::create()
                   .withGraph(std::move(graph))
                   .withOptions(PipelineOptions::stream())
                   .build();
  ASSERT_TRUE(built);
  auto pipeline = std::move(built.value());
  ASSERT_TRUE(pipeline.start());

  // 6 frames at batch size 4: one full batch, two frames stranded.
  for (int i = 0; i < 6; ++i) {
    ASSERT_TRUE(pipeline.pushInput("source", makePacket()));
  }
  ASSERT_TRUE(pipeline.signalEndOfStream("source"));
  ASSERT_TRUE(pipeline.waitForEndOfStream(k_wait));

  EXPECT_EQ(batcher->flushCalls(), 1);
  const auto batches = sink->batches();
  ASSERT_EQ(batches.size(), 2u) << "flush batch never reached the sink";
  EXPECT_EQ(batches[0], 4);
  EXPECT_EQ(batches[1], 2) << "residue batch had the wrong size";

  pipeline.stop();
}

TEST(EndOfStreamTest, FlushHookRunsExactlyOnce) {
  auto source = std::make_shared<PassNode>("source");
  auto batcher = std::make_shared<BatchingNode>("batcher", 100);
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(batcher);
  graph.addNode(sink);
  (void)graph.addEdge("source", "output", "batcher", "input");
  (void)graph.addEdge("batcher", "output", "sink", "input");

  auto built = Pipeline::create()
                   .withGraph(std::move(graph))
                   .withOptions(PipelineOptions::stream())
                   .build();
  ASSERT_TRUE(built);
  auto pipeline = std::move(built.value());
  ASSERT_TRUE(pipeline.start());

  ASSERT_TRUE(pipeline.pushInput("source", makePacket()));
  ASSERT_TRUE(pipeline.signalEndOfStream("source"));
  ASSERT_TRUE(pipeline.waitForEndOfStream(k_wait));
  std::this_thread::sleep_for(50ms); // Let any stray re-check land

  EXPECT_EQ(batcher->flushCalls(), 1);
  pipeline.stop();
}

TEST(EndOfStreamTest, ThrowingFlushHookDoesNotStrandDownstream) {
  auto source = std::make_shared<PassNode>("source");
  auto thrower = std::make_shared<ThrowingFlushNode>("thrower");
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(thrower);
  graph.addNode(sink);
  (void)graph.addEdge("source", "output", "thrower", "input");
  (void)graph.addEdge("thrower", "output", "sink", "input");

  auto built = Pipeline::create()
                   .withGraph(std::move(graph))
                   .withOptions(PipelineOptions::stream())
                   .build();
  ASSERT_TRUE(built);
  auto pipeline = std::move(built.value());
  ASSERT_TRUE(pipeline.start());

  ASSERT_TRUE(pipeline.pushInput("source", makePacket()));
  ASSERT_TRUE(pipeline.signalEndOfStream("source"));

  // The whole point: a broken flush must not deadlock the shutdown path.
  const auto waited = pipeline.waitForEndOfStream(k_wait);
  EXPECT_TRUE(waited) << "a throwing flush hook stranded EOS propagation";
  EXPECT_TRUE(sink->sawEos());

  pipeline.stop();
}

// Join semantics (§6.2, §6.3)

struct JoinFixture {
  std::shared_ptr<PassNode> left = std::make_shared<PassNode>("left");
  std::shared_ptr<PassNode> right = std::make_shared<PassNode>("right");
  std::shared_ptr<JoinNode> join = std::make_shared<JoinNode>("join");
  std::shared_ptr<SinkNode> sink = std::make_shared<SinkNode>("sink");
  Pipeline pipeline;

  Result<void> build(PipelineOptions options) {
    Graph graph;
    graph.addNode(left);
    graph.addNode(right);
    graph.addNode(join);
    graph.addNode(sink);
    (void)graph.addEdge("left", "output", "join", "left");
    (void)graph.addEdge("right", "output", "join", "right");
    (void)graph.addEdge("join", "output", "sink", "input");

    auto built = Pipeline::create()
                     .withGraph(std::move(graph))
                     .withOptions(options)
                     .build();
    if (!built) {
      return Result<void>::err(built.error());
    }
    pipeline = std::move(built.value());
    return pipeline.start();
  }
};

TEST(EndOfStreamTest, JoinWaitsForAllInputsBeforeForwardingEos) {
  // §6.2: EOS merge is conjunctive. One branch finishing must not
  // convince the sink that the pipeline is done.
  JoinFixture fx;
  auto options = PipelineOptions::stream();
  options.enable_sync_coordination = false;
  options.alignment_policy = AlignmentPolicy::FrameId;
  ASSERT_TRUE(fx.build(options));

  ASSERT_TRUE(fx.pipeline.signalEndOfStream("left"));
  std::this_thread::sleep_for(100ms);

  EXPECT_FALSE(fx.pipeline.isEndOfStreamReached())
      << "one closed branch must not end the stream";
  EXPECT_FALSE(fx.join->flushed());

  ASSERT_TRUE(fx.pipeline.signalEndOfStream("right"));
  ASSERT_TRUE(fx.pipeline.waitForEndOfStream(k_wait));
  EXPECT_TRUE(fx.join->flushed());
  EXPECT_TRUE(fx.sink->sawEos());

  fx.pipeline.stop();
}

TEST(EndOfStreamTest, ClosedBranchDoesNotBlockAlignedJoin) {
  // §6.3: a drained port leaves the pairing set. Without that, the live
  // branch's frames wait forever for a partner that cannot arrive.
  JoinFixture fx;
  auto options = PipelineOptions::stream();
  options.alignment_policy = AlignmentPolicy::StreamFrameId;
  ASSERT_TRUE(fx.build(options));

  // Matching ids so the two branches actually pair.
  ASSERT_TRUE(fx.pipeline.pushInput("left", makePacket(1)));
  ASSERT_TRUE(fx.pipeline.pushInput("right", makePacket(1)));
  ASSERT_TRUE(fx.pipeline.waitForDrain(0, k_wait));
  const int paired = fx.join->executions();
  EXPECT_GE(paired, 1) << "the two branches never paired to begin with";

  // Close the left branch, then keep feeding the right one. Its frames
  // have no partner any more, so they only move if the drained port has
  // left the pairing set.
  ASSERT_TRUE(fx.pipeline.signalEndOfStream("left"));
  std::this_thread::sleep_for(50ms);
  for (FrameId i = 2; i <= 4; ++i) {
    ASSERT_TRUE(fx.pipeline.pushInput("right", makePacket(i)));
  }
  ASSERT_TRUE(fx.pipeline.waitForDrain(0, k_wait));

  EXPECT_GT(fx.join->executions(), paired)
      << "join stalled after one branch reached EOS";

  ASSERT_TRUE(fx.pipeline.signalEndOfStream("right"));
  ASSERT_TRUE(fx.pipeline.waitForEndOfStream(k_wait));
  fx.pipeline.stop();
}

// Latch durability (the reason this is not an in-band packet - §4)

class DropPolicyEosTest : public ::testing::TestWithParam<const char *> {};

TEST_P(DropPolicyEosTest, EosSurvivesQueuePressure) {
  // An in-band EOS packet would be evicted by DropHead/KeepLatest, or
  // refused outright by DropTail, exactly in this scenario.
  auto source = std::make_shared<PassNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(sink);
  (void)graph.addEdge("source", "output", "sink", "input");

  auto options = PipelineOptions::stream();
  options.queue_capacity = 2; // Tiny on purpose
  options.drop_strategy = GetParam();

  auto built = Pipeline::create()
                   .withGraph(std::move(graph))
                   .withOptions(options)
                   .build();
  ASSERT_TRUE(built);
  auto pipeline = std::move(built.value());
  ASSERT_TRUE(pipeline.start());

  // Flood well past capacity; some frames are legitimately dropped.
  for (int i = 0; i < 500; ++i) {
    (void)pipeline.pushInput("source", makePacket());
  }
  ASSERT_TRUE(pipeline.signalEndOfStream("source"));

  const auto waited = pipeline.waitForEndOfStream(k_wait);
  EXPECT_TRUE(waited) << "EOS lost under drop policy " << GetParam() << ": "
                      << waited.error().toString();
  EXPECT_TRUE(sink->sawEos());

  pipeline.stop();
}

INSTANTIATE_TEST_SUITE_P(AllDropPolicies, DropPolicyEosTest,
                         ::testing::Values("DropHead", "DropTail",
                                           "KeepLatest"));

// Contract errors

TEST(EndOfStreamTest, PushAfterEosIsRejected) {
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));

  auto pushed = fx.pipeline.pushInput("source", makePacket());
  ASSERT_FALSE(pushed);
  EXPECT_EQ(pushed.error().code(), ErrorCode::EndOfStreamSignaled);

  fx.pipeline.stop();
}

TEST(EndOfStreamTest, DoubleSignalIsRejected) {
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));
  auto again = fx.pipeline.signalEndOfStream("source");
  ASSERT_FALSE(again);
  EXPECT_EQ(again.error().code(), ErrorCode::EndOfStreamSignaled);

  fx.pipeline.stop();
}

TEST(EndOfStreamTest, UnknownNodeAndPortAreRejected) {
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  auto bad_node = fx.pipeline.signalEndOfStream("nope");
  ASSERT_FALSE(bad_node);
  EXPECT_EQ(bad_node.error().code(), ErrorCode::NodeNotFound);

  auto bad_port = fx.pipeline.signalEndOfStream("source", "nope");
  ASSERT_FALSE(bad_port);
  EXPECT_EQ(bad_port.error().code(), ErrorCode::PortNotFound);

  fx.pipeline.stop();
}

TEST(EndOfStreamTest, SignalRequiresStreamingMode) {
  auto source = std::make_shared<PassNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(sink);
  (void)graph.addEdge("source", "output", "sink", "input");

  auto built = Pipeline::create()
                   .withGraph(std::move(graph))
                   .withOptions(PipelineOptions::batch())
                   .build();
  ASSERT_TRUE(built);
  auto pipeline = std::move(built.value());

  auto result = pipeline.signalEndOfStream("source");
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), ErrorCode::NotStreaming);
  EXPECT_FALSE(pipeline.isEndOfStreamReached());
}

// Observer and reset

class EosObserver : public IPipelineObserver {
public:
  void onEndOfStream() override { m_count.fetch_add(1); }
  int count() const { return m_count.load(); }

private:
  std::atomic<int> m_count{0};
};

TEST(EndOfStreamTest, ObserverIsNotifiedOnce) {
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  auto observer = std::make_shared<EosObserver>();
  fx.pipeline.addObserver(observer);

  ASSERT_TRUE(fx.pipeline.pushInput("source", makePacket()));
  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));
  ASSERT_TRUE(fx.pipeline.waitForEndOfStream(k_wait));
  std::this_thread::sleep_for(50ms);

  EXPECT_EQ(observer->count(), 1);
  fx.pipeline.stop();
}

TEST(EndOfStreamTest, ResetClearsEosSoThePipelineCanRunAgain) {
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  ASSERT_TRUE(fx.pipeline.pushInput("source", makePacket()));
  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));
  ASSERT_TRUE(fx.pipeline.waitForEndOfStream(k_wait));
  fx.pipeline.stop();

  fx.pipeline.reset();
  EXPECT_FALSE(fx.pipeline.isEndOfStreamReached());

  // The latch must be gone, so the port accepts data again.
  ASSERT_TRUE(fx.pipeline.start());
  EXPECT_TRUE(fx.pipeline.pushInput("source", makePacket()));
  ASSERT_TRUE(fx.pipeline.signalEndOfStream("source"));
  ASSERT_TRUE(fx.pipeline.waitForEndOfStream(k_wait));

  fx.pipeline.stop();
}

TEST(EndOfStreamTest, WaitTimesOutWhenNoEosIsSignaled) {
  LinearFixture fx;
  ASSERT_TRUE(fx.build());

  auto waited = fx.pipeline.waitForEndOfStream(100ms);
  ASSERT_FALSE(waited);
  EXPECT_EQ(waited.error().code(), ErrorCode::ExecutionTimeout);

  fx.pipeline.stop();
}

} // namespace
} // namespace ai_pipe
