#include "ai_pipe/context.hpp"
#include "logger.hpp"
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace ai_pipe;
using namespace std::chrono_literals;

// ExecutionId Tests

TEST(ExecutionIdTest, DefaultConstruction) {
  ExecutionId id;
  EXPECT_EQ(id.value, 0);
}

TEST(ExecutionIdTest, Equality) {
  ExecutionId id1{100};
  ExecutionId id2{100};
  ExecutionId id3{200};

  EXPECT_EQ(id1, id2);
  EXPECT_NE(id1, id3);
}

TEST(ExecutionIdTest, Generate) {
  auto id1 = ExecutionId::generate();
  auto id2 = ExecutionId::generate();

  EXPECT_NE(id1, id2);
  EXPECT_GT(id1.value, 0);
  EXPECT_GT(id2.value, 0);
}

TEST(ExecutionIdTest, ToString) {
  ExecutionId id{12345};
  std::string str = id.toString();

  // toString returns hex representation
  EXPECT_FALSE(str.empty());
  EXPECT_EQ(str.length(), 16); // 16 hex digits
}

// NodeMetrics Tests

TEST(NodeMetricsTest, DefaultConstruction) {
  NodeMetrics metrics;

  EXPECT_TRUE(metrics.node_name.empty());
  EXPECT_EQ(metrics.duration.count(), 0);
  EXPECT_FALSE(metrics.success);
}

TEST(NodeMetricsTest, DurationMs) {
  NodeMetrics metrics;
  metrics.duration = std::chrono::microseconds{1500};

  EXPECT_DOUBLE_EQ(metrics.durationMs(), 1.5);
}

// ExecutionMetrics Tests

TEST(ExecutionMetricsTest, SuccessRateZeroExecutions) {
  ExecutionMetrics metrics;
  metrics.nodes_executed = 0;
  metrics.nodes_failed = 0;

  EXPECT_DOUBLE_EQ(metrics.successRate(), 0.0);
}

TEST(ExecutionMetricsTest, SuccessRatePerfect) {
  ExecutionMetrics metrics;
  metrics.nodes_executed = 10;
  metrics.nodes_failed = 0;

  EXPECT_DOUBLE_EQ(metrics.successRate(), 100.0);
}

TEST(ExecutionMetricsTest, SuccessRatePartial) {
  ExecutionMetrics metrics;
  metrics.nodes_executed = 10;
  metrics.nodes_failed = 3;

  EXPECT_DOUBLE_EQ(metrics.successRate(), 70.0);
}

// CancellationToken Tests

TEST(CancellationTokenTest, DefaultNotCancelled) {
  CancellationToken token;
  EXPECT_FALSE(token.isCancelled());
}

TEST(CancellationTokenTest, Cancel) {
  CancellationToken token;
  token.cancel();
  EXPECT_TRUE(token.isCancelled());
}

TEST(CancellationTokenTest, Reset) {
  CancellationToken token;
  token.cancel();
  EXPECT_TRUE(token.isCancelled());

  token.reset();
  EXPECT_FALSE(token.isCancelled());
}

TEST(CancellationTokenTest, CancelIsObservable) {
  // throwIfCancelled was removed; polling isCancelled() is the
  // only (cooperative) checkpoint style.
  CancellationToken token;
  EXPECT_FALSE(token.isCancelled());
  token.cancel();
  EXPECT_TRUE(token.isCancelled());
}

TEST(CancellationTokenTest, ThreadSafety) {
  CancellationToken token;
  std::atomic<int> check_count{0};
  std::atomic<bool> start{false};

  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&token, &check_count, &start]() {
      while (!start.load()) {
        std::this_thread::yield();
      }
      for (int j = 0; j < 1000; ++j) {
        if (token.isCancelled()) {
          check_count.fetch_add(1);
        }
        std::this_thread::yield();
      }
    });
  }

  // Cancel before starting threads' main work
  token.cancel();
  start.store(true);

  for (auto &t : threads) {
    t.join();
  }

  // All threads should detect cancellation multiple times
  EXPECT_GT(check_count.load(), 0);
}

// ProgressReporter Tests

TEST(ProgressReporterTest, DefaultProgress) {
  ProgressReporter reporter;
  EXPECT_DOUBLE_EQ(reporter.progress(), 0.0);
  EXPECT_TRUE(reporter.message().empty());
}

TEST(ProgressReporterTest, ReportProgress) {
  ProgressReporter reporter;
  reporter.report(0.5, "halfway");

  EXPECT_DOUBLE_EQ(reporter.progress(), 0.5);
  EXPECT_EQ(reporter.message(), "halfway");
}

TEST(ProgressReporterTest, Callback) {
  double received_progress = 0.0;
  std::string received_message;

  ProgressReporter reporter(
      [&received_progress, &received_message](double p, const std::string &m) {
        received_progress = p;
        received_message = m;
      });

  reporter.report(0.75, "three quarters");

  EXPECT_DOUBLE_EQ(received_progress, 0.75);
  EXPECT_EQ(received_message, "three quarters");
}

// Logger Adapter Tests

TEST(NullLoggerAdapterTest, DoesNotCrash) {
  NullLoggerAdapter logger;

  EXPECT_NO_THROW(logger.log(PipeLogLevel::KInfo, "node", "message"));
  EXPECT_NO_THROW(logger.log(PipeLogLevel::KError, "", ""));
}

TEST(MemoryLoggerAdapterTest, CapturesLogs) {
  MemoryLoggerAdapter logger;

  logger.log(PipeLogLevel::KInfo, "node1", "info message");
  logger.log(PipeLogLevel::KError, "node2", "error message");

  auto entries = logger.entries();

  ASSERT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].level, PipeLogLevel::KInfo);
  EXPECT_EQ(entries[0].node_name, "node1");
  EXPECT_EQ(entries[0].message, "info message");
  EXPECT_EQ(entries[1].level, PipeLogLevel::KError);
  EXPECT_EQ(entries[1].node_name, "node2");
}

TEST(MemoryLoggerAdapterTest, Clear) {
  MemoryLoggerAdapter logger;

  logger.log(PipeLogLevel::KInfo, "node", "message");
  EXPECT_EQ(logger.entries().size(), 1);

  logger.clear();
  EXPECT_TRUE(logger.entries().empty());
}

TEST(MemoryLoggerAdapterTest, ThreadSafety) {
  MemoryLoggerAdapter logger;

  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&logger, i]() {
      for (int j = 0; j < 100; ++j) {
        logger.log(PipeLogLevel::KDebug, "thread_" + std::to_string(i),
                   "msg_" + std::to_string(j));
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(logger.entries().size(), 1000);
}

// PipelineContext Tests

class PipelineContextTest : public ::testing::Test {
protected:
  void SetUp() override { m_context = std::make_shared<PipelineContext>(); }

  std::shared_ptr<PipelineContext> m_context;
};

// Resource Management Tests

TEST_F(PipelineContextTest, SetAndGetResource) {
  auto resource = std::make_shared<std::string>("test_resource");
  m_context->setResource("my_resource", resource);

  auto retrieved = m_context->getResource<std::string>("my_resource");
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(*retrieved, "test_resource");
}

TEST_F(PipelineContextTest, GetNonexistentResourceReturnsNull) {
  auto resource = m_context->getResource<std::string>("nonexistent");
  EXPECT_EQ(resource, nullptr);
}

TEST_F(PipelineContextTest, GetResourceWrongTypeReturnsNull) {
  auto resource = std::make_shared<int>(42);
  m_context->setResource("int_resource", resource);

  auto wrong_type = m_context->getResource<std::string>("int_resource");
  EXPECT_EQ(wrong_type, nullptr);
}

TEST_F(PipelineContextTest, HasResource) {
  auto resource = std::make_shared<int>(42);
  m_context->setResource("my_int", resource);

  EXPECT_TRUE(m_context->hasResource("my_int"));
  EXPECT_FALSE(m_context->hasResource("nonexistent"));
}

TEST_F(PipelineContextTest, RemoveResource) {
  auto resource = std::make_shared<int>(42);
  m_context->setResource("to_remove", resource);

  EXPECT_TRUE(m_context->hasResource("to_remove"));
  EXPECT_TRUE(m_context->removeResource("to_remove"));
  EXPECT_FALSE(m_context->hasResource("to_remove"));
  EXPECT_FALSE(m_context->removeResource("to_remove"));
}

TEST_F(PipelineContextTest, ResourceNames) {
  m_context->setResource("res1", std::make_shared<int>(1));
  m_context->setResource("res2", std::make_shared<int>(2));
  m_context->setResource("res3", std::make_shared<int>(3));

  auto names = m_context->resourceNames();
  EXPECT_EQ(names.size(), 3);
}

// Service Management Tests

struct ITestService {
  virtual ~ITestService() = default;
  virtual int getValue() const = 0;
};

struct TestServiceImpl : public ITestService {
  int getValue() const override { return 42; }
};

TEST_F(PipelineContextTest, SetAndGetService) {
  auto service = std::make_shared<TestServiceImpl>();
  m_context->setService<ITestService>(service);

  auto retrieved = m_context->getService<ITestService>();
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->getValue(), 42);
}

TEST_F(PipelineContextTest, GetNonexistentServiceReturnsNull) {
  auto service = m_context->getService<ITestService>();
  EXPECT_EQ(service, nullptr);
}

TEST_F(PipelineContextTest, HasService) {
  auto service = std::make_shared<TestServiceImpl>();
  m_context->setService<ITestService>(service);

  EXPECT_TRUE(m_context->hasService<ITestService>());
  EXPECT_FALSE(m_context->hasService<std::string>());
}

// Configuration Tests

TEST_F(PipelineContextTest, SetAndGetConfig) {
  m_context->setConfig("batch_size", 32);

  auto value = m_context->getConfig<int>("batch_size");
  EXPECT_EQ(value, 32);
}

TEST_F(PipelineContextTest, GetConfigWithDefault) {
  // Non-existent key returns default
  auto value = m_context->getConfig<int>("nonexistent", 100);
  EXPECT_EQ(value, 100);

  // Existing key returns actual value
  m_context->setConfig("existing", 50);
  value = m_context->getConfig<int>("existing", 100);
  EXPECT_EQ(value, 50);
}

TEST_F(PipelineContextTest, HasConfig) {
  m_context->setConfig("key", std::string("value"));

  EXPECT_TRUE(m_context->hasConfig("key"));
  EXPECT_FALSE(m_context->hasConfig("nonexistent"));
}

// Execution Tracking Tests

TEST_F(PipelineContextTest, InitialStateNotExecuting) {
  EXPECT_FALSE(m_context->isExecuting());
}

TEST_F(PipelineContextTest, BeginAndEndExecution) {
  m_context->beginExecution();
  EXPECT_TRUE(m_context->isExecuting());

  m_context->endExecution();
  EXPECT_FALSE(m_context->isExecuting());
}

TEST_F(PipelineContextTest, ExecutionIdChangesOnBegin) {
  auto id1 = m_context->executionId();

  m_context->beginExecution();
  auto id2 = m_context->executionId();

  EXPECT_NE(id1, id2);
}

// Metrics Tests

TEST_F(PipelineContextTest, NodeMetricsTracking) {
  m_context->beginExecution();

  m_context->beginNodeExecution("node1");
  std::this_thread::sleep_for(10ms);
  m_context->endNodeExecution("node1", true, "");

  m_context->beginNodeExecution("node2");
  m_context->endNodeExecution("node2", false, "error occurred");

  auto metrics = m_context->executionMetrics();

  EXPECT_EQ(metrics.nodes_executed, 2);
  EXPECT_EQ(metrics.nodes_failed, 1);
  EXPECT_EQ(metrics.node_metrics.size(), 2);
}

TEST_F(PipelineContextTest, NodeMetricsQuery) {
  m_context->beginExecution();
  m_context->beginNodeExecution("test_node");
  m_context->endNodeExecution("test_node", true, "");

  auto metrics = m_context->nodeMetrics("test_node");
  ASSERT_TRUE(metrics.has_value());
  EXPECT_EQ(metrics->node_name, "test_node");
  EXPECT_TRUE(metrics->success);
}

// Cancellation Tests

TEST_F(PipelineContextTest, CancellationRequest) {
  EXPECT_FALSE(m_context->isCancellationRequested());

  m_context->requestCancellation();
  EXPECT_TRUE(m_context->isCancellationRequested());
  EXPECT_TRUE(m_context->cancellation().isCancelled());
}

// Logger Adapter Tests

TEST_F(PipelineContextTest, LoggerAdapter) {
  auto logger = std::make_shared<MemoryLoggerAdapter>();
  m_context->setLoggerAdapter(logger);

  m_context->logInfo("node1", "info message");
  m_context->logError("node2", "error message");
  m_context->logDebug("node3", "debug message");
  m_context->logWarning("node4", "warning message");

  auto entries = logger->entries();
  EXPECT_EQ(entries.size(), 4);
}

TEST_F(PipelineContextTest, LogWithoutAdapter) {
  EXPECT_NO_THROW(m_context->logInfo("node", "message"));
}

// Progress Reporting Tests

TEST_F(PipelineContextTest, OverallProgress) {
  m_context->reportProgress(0.5, "halfway");
  EXPECT_DOUBLE_EQ(m_context->overallProgress(), 0.5);
}

TEST_F(PipelineContextTest, NodeProgressReporter) {
  auto &reporter = m_context->progressReporter("node1");
  reporter.report(0.75, "three quarters");

  EXPECT_DOUBLE_EQ(reporter.progress(), 0.75);
}

TEST_F(PipelineContextTest, ProgressCallback) {
  double received_progress = 0.0;

  m_context->setProgressCallback(
      [&received_progress](double p, const std::string &) {
        received_progress = p;
      });

  m_context->reportProgress(0.8, "test");
  EXPECT_DOUBLE_EQ(received_progress, 0.8);
}

// User Data Tests

TEST_F(PipelineContextTest, SetAndGetUserData) {
  m_context->setUserData("key1", std::string("value1"));
  m_context->setUserData("key2", 42);

  auto str_value = m_context->getUserData<std::string>("key1");
  auto int_value = m_context->getUserData<int>("key2");

  ASSERT_TRUE(str_value.has_value());
  ASSERT_TRUE(int_value.has_value());
  EXPECT_EQ(*str_value, "value1");
  EXPECT_EQ(*int_value, 42);
}

TEST_F(PipelineContextTest, GetUserDataNonexistent) {
  auto value = m_context->getUserData<int>("nonexistent");
  EXPECT_FALSE(value.has_value());
}

TEST_F(PipelineContextTest, RemoveUserData) {
  m_context->setUserData("key", 42);
  EXPECT_TRUE(m_context->getUserData<int>("key").has_value());

  EXPECT_TRUE(m_context->removeUserData("key"));
  EXPECT_FALSE(m_context->getUserData<int>("key").has_value());
}

// Reset Tests

TEST_F(PipelineContextTest, ResetExecution) {
  m_context->beginExecution();
  m_context->beginNodeExecution("node");
  m_context->endNodeExecution("node", true, "");
  m_context->requestCancellation();
  m_context->reportProgress(0.5, "test");

  m_context->resetExecution();

  EXPECT_FALSE(m_context->isExecuting());
  EXPECT_FALSE(m_context->isCancellationRequested());
  EXPECT_DOUBLE_EQ(m_context->overallProgress(), 0.0);
}

TEST_F(PipelineContextTest, FullReset) {
  auto resource = std::make_shared<int>(42);
  m_context->setResource("res", resource);
  m_context->setConfig("cfg", 100);
  m_context->setUserData("data", std::string("test"));

  m_context->reset();

  EXPECT_FALSE(m_context->hasResource("res"));
  EXPECT_FALSE(m_context->hasConfig("cfg"));
  EXPECT_FALSE(m_context->getUserData<std::string>("data").has_value());
}

// Thread Safety Tests

TEST_F(PipelineContextTest, ConcurrentResourceAccess) {
  std::atomic<int> success_count{0};

  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([this, i, &success_count]() {
      for (int j = 0; j < 100; ++j) {
        auto res = std::make_shared<int>(i * 100 + j);
        m_context->setResource(
            "res_" + std::to_string(i) + "_" + std::to_string(j), res);
        success_count.fetch_add(1);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count.load(), 1000);
}

TEST_F(PipelineContextTest, ConcurrentConfigAccess) {
  std::vector<std::thread> threads;

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([this, i]() {
      for (int j = 0; j < 100; ++j) {
        m_context->setConfig(
            "key_" + std::to_string(i) + "_" + std::to_string(j), i * j);
        (void)m_context->getConfig<int>("key_" + std::to_string(i) + "_" +
                                        std::to_string(j));
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }
}

// ScopedNodeExecution Tests

class ScopedNodeExecutionTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_context = std::make_shared<PipelineContext>();
    m_context->beginExecution();
  }

  std::shared_ptr<PipelineContext> m_context;
};

TEST_F(ScopedNodeExecutionTest, RAIIExecution) {
  {
    ScopedNodeExecution exec(m_context, "test_node");
    // Node is being executed
  }
  // Node execution should be ended

  auto metrics = m_context->executionMetrics();
  EXPECT_EQ(metrics.nodes_executed, 1);
  EXPECT_EQ(metrics.nodes_failed, 0);
}

TEST_F(ScopedNodeExecutionTest, SetFailed) {
  {
    ScopedNodeExecution exec(m_context, "failing_node");
    exec.setFailed("something went wrong");
  }

  auto metrics = m_context->executionMetrics();
  EXPECT_EQ(metrics.nodes_executed, 1);
  EXPECT_EQ(metrics.nodes_failed, 1);
}

TEST_F(ScopedNodeExecutionTest, CancellationRequested) {
  ScopedNodeExecution exec(m_context, "test_node");

  EXPECT_FALSE(exec.cancellationRequested());

  m_context->requestCancellation();

  EXPECT_TRUE(exec.cancellationRequested());
}

TEST_F(ScopedNodeExecutionTest, ReportProgress) {
  ScopedNodeExecution exec(m_context, "test_node");
  exec.reportProgress(0.5, "halfway");

  auto &reporter = m_context->progressReporter("test_node");
  EXPECT_DOUBLE_EQ(reporter.progress(), 0.5);
}

TEST_F(ScopedNodeExecutionTest, Logging) {
  auto logger = std::make_shared<MemoryLoggerAdapter>();
  m_context->setLoggerAdapter(logger);

  {
    ScopedNodeExecution exec(m_context, "test_node");
    exec.logInfo("info message");
    exec.logError("error message");
    exec.logDebug("debug message");
    exec.logWarning("warning message");
  }

  auto entries = logger->entries();
  EXPECT_EQ(entries.size(), 4);

  for (const auto &entry : entries) {
    EXPECT_EQ(entry.node_name, "test_node");
  }
}

TEST_F(ScopedNodeExecutionTest, NullContext) {
  ScopedNodeExecution exec(nullptr, "test_node");
  EXPECT_FALSE(exec.cancellationRequested());
  EXPECT_NO_THROW(exec.reportProgress(0.5, "test"));
  EXPECT_NO_THROW(exec.logInfo("message"));
}

// Move Semantics Tests

TEST(PipelineContextMoveTest, MoveConstruction) {
  auto ctx = std::make_shared<PipelineContext>();
  ctx->setResource("res", std::make_shared<int>(42));
  ctx->setConfig("cfg", 100);

  PipelineContext moved(std::move(*ctx));

  EXPECT_TRUE(moved.hasResource("res"));
  EXPECT_TRUE(moved.hasConfig("cfg"));
}

TEST(PipelineContextMoveTest, MoveAssignment) {
  auto ctx = std::make_shared<PipelineContext>();
  ctx->setResource("res", std::make_shared<int>(42));

  PipelineContext moved;
  moved = std::move(*ctx);

  EXPECT_TRUE(moved.hasResource("res"));
}

// Engine log bridge

namespace {

class CapturingAdapter : public ILoggerAdapter {
public:
  void log(PipeLogLevel level, const std::string &,
           const std::string &message) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_entries.emplace_back(level, message);
  }

  std::vector<std::pair<PipeLogLevel, std::string>> entries() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_entries;
  }

private:
  mutable std::mutex m_mutex;
  std::vector<std::pair<PipeLogLevel, std::string>> m_entries;
};

} // namespace

TEST(EngineLogBridgeTest, FrameworkLogsReachAdapter) {
  auto context = std::make_shared<PipelineContext>();
  auto adapter = std::make_shared<CapturingAdapter>();
  context->setLoggerAdapter(adapter);
  context->attachEngineLogs();

  LOG_ERROR_S << "bridge-test-message";

  bool found = false;
  for (const auto &[level, message] : adapter->entries()) {
    if (message.find("bridge-test-message") != std::string::npos) {
      EXPECT_EQ(level, PipeLogLevel::KError);
      found = true;
    }
  }
  EXPECT_TRUE(found) << "engine log must reach the context adapter";

  context->detachEngineLogs();
  LOG_ERROR_S << "post-detach-message";
  for (const auto &[level, message] : adapter->entries()) {
    EXPECT_EQ(message.find("post-detach-message"), std::string::npos)
        << "detached bridge must not forward";
  }
}

TEST(EngineLogBridgeTest, DestroyedContextDropsBridge) {
  auto adapter = std::make_shared<CapturingAdapter>();
  {
    auto context = std::make_shared<PipelineContext>();
    context->setLoggerAdapter(adapter);
    context->attachEngineLogs();
  } // context destroyed -> bridge removed in destructor

  LOG_ERROR_S << "after-context-destroyed";
  for (const auto &[level, message] : adapter->entries()) {
    EXPECT_EQ(message.find("after-context-destroyed"), std::string::npos);
  }
}
