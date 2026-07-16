/**
 * @file test_trace.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Execution tracing hook tests (F7)
 *
 * Validates that an injected ITraceSink receives per-frame span events
 * for every lifecycle phase (enqueue/schedule/execute/propagate) with
 * correct frame identity, that the sink cannot change while running,
 * and that ChromeTraceSink serializes the Chrome Trace Event format.
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/trace.hpp"
#include "helper_nodes.hpp"
#include <gtest/gtest.h>
#include <mutex>

using namespace ai_pipe;
using namespace std::chrono_literals;

namespace ai_pipe_unit_test::trace {

/// Sink copying every event (TraceEvent string_views are call-scoped)
class RecordingSink : public ITraceSink {
public:
  struct Copied {
    TracePhase phase;
    std::string node;
    std::string detail;
    FrameId frame_id;
    StreamId stream_id;
    std::chrono::microseconds duration;
    std::uint64_t thread_id;
  };

  void onEvent(const TraceEvent &event) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_events.push_back(Copied{event.phase, std::string(event.node),
                              std::string(event.detail), event.frame_id,
                              event.stream_id, event.duration,
                              event.thread_id});
  }

  [[nodiscard]] std::vector<Copied> events() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_events;
  }

  [[nodiscard]] std::size_t countPhase(TracePhase phase) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::size_t count = 0;
    for (const auto &event : m_events) {
      if (event.phase == phase) {
        ++count;
      }
    }
    return count;
  }

private:
  mutable std::mutex m_mutex;
  std::vector<Copied> m_events;
};

class TraceTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_graph = std::make_unique<Graph>();
    m_src = std::make_shared<PassThroughNode>("src");
    m_sink = std::make_shared<SinkNode>("sink");
    m_graph->addNode(m_src);
    m_graph->addNode(m_sink);
    m_graph->addEdge("src", "output", "sink", "input");
  }

  static PortDataPtr makeFrame(FrameId frame) {
    auto packet = std::make_shared<PortData>();
    packet->id = frame;
    return packet;
  }

  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<PassThroughNode> m_src;
  std::shared_ptr<SinkNode> m_sink;
};

TEST_F(TraceTest, SinkReceivesAllLifecyclePhases) {
  auto engine = createStreamEngine(2, 16);
  auto trace_sink = std::make_shared<RecordingSink>();
  ASSERT_TRUE(engine->setTraceSink(trace_sink).isOk());
  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());
  ASSERT_TRUE(engine->startStreaming().isOk());

  (void)engine->pushInput("src", makeFrame(1));
  (void)engine->pushInput("src", makeFrame(2));

  ASSERT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  engine->stopStreaming(false);

  EXPECT_GE(trace_sink->countPhase(TracePhase::Enqueue), 2u)
      << "each push and each propagation should enqueue";
  EXPECT_GE(trace_sink->countPhase(TracePhase::Schedule), 2u);
  EXPECT_GE(trace_sink->countPhase(TracePhase::Execute), 4u)
      << "two nodes x two frames";
  EXPECT_GE(trace_sink->countPhase(TracePhase::Propagate), 2u);

  // Execute spans must carry the frame identity of their inputs.
  std::vector<FrameId> src_frames;
  for (const auto &event : trace_sink->events()) {
    if (event.phase == TracePhase::Execute && event.node == "src") {
      src_frames.push_back(event.frame_id);
    }
  }
  EXPECT_EQ(src_frames, (std::vector<FrameId>{1, 2}));
}

TEST_F(TraceTest, TraceSinkIsImmutableWhileRunning) {
  auto engine = createStreamEngine(2, 16);
  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());
  ASSERT_TRUE(engine->startStreaming().isOk());

  auto trace_sink = std::make_shared<RecordingSink>();
  auto result = engine->setTraceSink(trace_sink);
  EXPECT_FALSE(result.isOk());
  EXPECT_EQ(result.error().code(), ErrorCode::InvalidState);

  engine->stopStreaming(false);
  EXPECT_TRUE(engine->setTraceSink(trace_sink).isOk());
}

TEST_F(TraceTest, ChromeTraceSinkSerializesEvents) {
  auto engine = createStreamEngine(2, 16);
  auto trace_sink = std::make_shared<ChromeTraceSink>();
  ASSERT_TRUE(engine->setTraceSink(trace_sink).isOk());
  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());
  ASSERT_TRUE(engine->startStreaming().isOk());

  (void)engine->pushInput("src", makeFrame(7));

  ASSERT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  engine->stopStreaming(false);

  ASSERT_GT(trace_sink->eventCount(), 0u);
  const std::string json = trace_sink->toJson();

  EXPECT_EQ(json.rfind("{\"traceEvents\":[", 0), 0u)
      << "must open a Chrome Trace Event document";
  EXPECT_EQ(json.substr(json.size() - 2), "]}");
  EXPECT_NE(json.find("\"cat\":\"execute\""), std::string::npos);
  EXPECT_NE(json.find("\"cat\":\"enqueue\""), std::string::npos);
  EXPECT_NE(json.find("\"name\":\"src\""), std::string::npos);
  EXPECT_NE(json.find("\"frame_id\":7"), std::string::npos);
  EXPECT_NE(json.find("\"ph\":\"X\""), std::string::npos)
      << "span events must serialize as complete events";

  trace_sink->clear();
  EXPECT_EQ(trace_sink->eventCount(), 0u);
}

TEST_F(TraceTest, ChromeTraceSinkWritesFile) {
  ChromeTraceSink trace_sink;
  TraceEvent event;
  event.phase = TracePhase::Execute;
  event.node = "n\"1"; // must be escaped
  event.frame_id = 3;
  event.start = std::chrono::steady_clock::now();
  event.duration = 42us;
  trace_sink.onEvent(event);

  const std::string path = ::testing::TempDir() + "ai_pipe_trace_test.json";
  ASSERT_TRUE(trace_sink.writeFile(path).isOk());

  std::ifstream file(path);
  ASSERT_TRUE(file.good());
  std::stringstream content;
  content << file.rdbuf();
  EXPECT_EQ(content.str(), trace_sink.toJson());
  EXPECT_NE(content.str().find("n\\\"1"), std::string::npos);
}

} // namespace ai_pipe_unit_test::trace
