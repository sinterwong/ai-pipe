/**
 * @file test_error_handling.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Unit tests for the unified error handling refactoring
 * @version 1.0
 * @date 2026-02-12
 *
 * Tests cover:
 *   1. ErrorCode enum & string conversion
 *   2. Error class (construction, accessors, factories, comparison)
 *   3. Result<T> (success/error, value access, copy, move, valueOr)
 *   4. Result<void> specialization
 *   5. PushStatus (outcome, factories)
 *   7. EngineConfig / PipelineOptions factory methods
 *   8. ExecutionOutput structure
 *   9. IPipelineObserver / CallbackObserver dispatch
 *  10. LatencyHistogram bucketing
 *  11. Header compilation for all refactored files
 *
 * @copyright Copyright (c) 2026
 */

#include "ai_pipe/error.hpp"
#include "ai_pipe/execution_types.hpp"
#include "ai_pipe/pipeline.hpp"
#include <future>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace ai_pipe;

class ErrorCodeTest : public ::testing::Test {};

TEST_F(ErrorCodeTest, EnumValues) {
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::Ok), 0);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InternalError), 1);

  // Configuration range: 1xx
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InvalidArgument), 100);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InvalidState), 101);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::NotInitialized), 102);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::AlreadyInitialized), 103);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::GraphCycleDetected), 104);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::GraphEmpty), 105);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::InvalidConfiguration), 106);

  // Execution range: 2xx
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::AlreadyRunning), 200);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::ExecutionFailed), 201);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::ExecutionTimeout), 202);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::ExecutionStopped), 203);

  // Queue / Streaming range: 3xx
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::StreamingNotSupported), 300);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::NodeNotFound), 304);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::PortNotFound), 305);

  // Node-level range: 4xx
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::NodeException), 400);
  EXPECT_EQ(static_cast<uint16_t>(ErrorCode::NodeUnknownException), 401);
}

TEST_F(ErrorCodeTest, StringConversion) {
  EXPECT_STREQ(errorCodeToString(ErrorCode::Ok), "Ok");
  EXPECT_STREQ(errorCodeToString(ErrorCode::InternalError), "InternalError");
  EXPECT_STREQ(errorCodeToString(ErrorCode::InvalidArgument),
               "InvalidArgument");
  EXPECT_STREQ(errorCodeToString(ErrorCode::ExecutionFailed),
               "ExecutionFailed");
  EXPECT_STREQ(errorCodeToString(ErrorCode::NodeNotFound), "NodeNotFound");
  EXPECT_STREQ(errorCodeToString(ErrorCode::NodeException), "NodeException");
  EXPECT_STREQ(errorCodeToString(ErrorCode::QueueFull), "QueueFull");
}

class ErrorTest : public ::testing::Test {};

TEST_F(ErrorTest, DefaultConstructionIsOk) {
  Error e;
  EXPECT_TRUE(e.isOk());
  EXPECT_FALSE(static_cast<bool>(e));
  EXPECT_EQ(e.code(), ErrorCode::Ok);
  EXPECT_TRUE(e.message().empty());
  EXPECT_TRUE(e.nodeName().empty());
}

TEST_F(ErrorTest, CodeOnlyConstruction) {
  Error e(ErrorCode::ExecutionFailed);
  EXPECT_FALSE(e.isOk());
  EXPECT_TRUE(static_cast<bool>(e));
  EXPECT_EQ(e.code(), ErrorCode::ExecutionFailed);
  EXPECT_TRUE(e.message().empty());
}

TEST_F(ErrorTest, CodeAndMessageConstruction) {
  Error e(ErrorCode::InvalidArgument, "bad input");
  EXPECT_EQ(e.code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(e.message(), "bad input");
  EXPECT_TRUE(e.nodeName().empty());
}

TEST_F(ErrorTest, FullConstruction) {
  Error e(ErrorCode::NodeException, "division by zero", "math_node");
  EXPECT_EQ(e.code(), ErrorCode::NodeException);
  EXPECT_EQ(e.message(), "division by zero");
  EXPECT_EQ(e.nodeName(), "math_node");
}

TEST_F(ErrorTest, ToString) {
  Error e1(ErrorCode::Ok);
  EXPECT_EQ(e1.toString(), "Ok");

  Error e2(ErrorCode::ExecutionFailed, "timed out");
  EXPECT_EQ(e2.toString(), "ExecutionFailed: timed out");

  Error e3(ErrorCode::NodeException, "null ptr", "detector");
  EXPECT_EQ(e3.toString(), "NodeException: null ptr [node: detector]");
}

TEST_F(ErrorTest, ComparisonWithErrorCode) {
  Error e(ErrorCode::NotInitialized, "engine not ready");
  EXPECT_TRUE(e == ErrorCode::NotInitialized);
  EXPECT_FALSE(e == ErrorCode::Ok);
  EXPECT_TRUE(e != ErrorCode::Ok);
  EXPECT_FALSE(e != ErrorCode::NotInitialized);
}

TEST_F(ErrorTest, OkFactory) {
  auto e = Error::ok();
  EXPECT_TRUE(e.isOk());
  EXPECT_EQ(e.code(), ErrorCode::Ok);
}

TEST_F(ErrorTest, InvalidArgumentFactory) {
  auto e = Error::invalidArgument("null pointer");
  EXPECT_EQ(e.code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(e.message(), "null pointer");
}

TEST_F(ErrorTest, ExecutionFailedFactory) {
  auto e = Error::executionFailed("crash", "node_A");
  EXPECT_EQ(e.code(), ErrorCode::ExecutionFailed);
  EXPECT_EQ(e.message(), "crash");
  EXPECT_EQ(e.nodeName(), "node_A");
}

TEST_F(ErrorTest, NodeNotFoundFactory) {
  auto e = Error::nodeNotFound("missing_node");
  EXPECT_EQ(e.code(), ErrorCode::NodeNotFound);
  EXPECT_EQ(e.nodeName(), "missing_node");
  EXPECT_NE(e.message().find("missing_node"), std::string::npos);
}

TEST_F(ErrorTest, PortNotFoundFactory) {
  auto e = Error::portNotFound("output2", "detector");
  EXPECT_EQ(e.code(), ErrorCode::PortNotFound);
  EXPECT_EQ(e.nodeName(), "detector");
  EXPECT_NE(e.message().find("output2"), std::string::npos);
  EXPECT_NE(e.message().find("detector"), std::string::npos);
}

TEST_F(ErrorTest, CopySemantics) {
  Error original(ErrorCode::ExecutionTimeout, "slow", "heavy_node");
  Error copy = original;
  EXPECT_EQ(copy.code(), ErrorCode::ExecutionTimeout);
  EXPECT_EQ(copy.message(), "slow");
  EXPECT_EQ(copy.nodeName(), "heavy_node");

  // Original unchanged
  EXPECT_EQ(original.code(), ErrorCode::ExecutionTimeout);
}

TEST_F(ErrorTest, MoveSemantics) {
  Error original(ErrorCode::QueueFull, "backpressure");
  Error moved = std::move(original);
  EXPECT_EQ(moved.code(), ErrorCode::QueueFull);
  EXPECT_EQ(moved.message(), "backpressure");
}

class ResultValueTest : public ::testing::Test {};

TEST_F(ResultValueTest, SuccessConstruction) {
  Result<int> r(42);
  EXPECT_TRUE(r.isOk());
  EXPECT_FALSE(r.isErr());
  EXPECT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value(), 42);
  EXPECT_EQ(r.errorCode(), ErrorCode::Ok);
}

TEST_F(ResultValueTest, OkFactory) {
  auto r = Result<std::string>::ok("hello");
  EXPECT_TRUE(r);
  EXPECT_EQ(r.value(), "hello");
}

TEST_F(ResultValueTest, ErrorConstruction) {
  Error err(ErrorCode::NotInitialized, "not ready");
  Result<int> r(err);
  EXPECT_FALSE(r.isOk());
  EXPECT_TRUE(r.isErr());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error().code(), ErrorCode::NotInitialized);
  EXPECT_EQ(r.error().message(), "not ready");
  EXPECT_EQ(r.errorCode(), ErrorCode::NotInitialized);
}

TEST_F(ResultValueTest, ErrFactoryWithErrorObject) {
  auto r = Result<int>::err(Error(ErrorCode::InvalidArgument, "bad"));
  EXPECT_FALSE(r);
  EXPECT_EQ(r.error().code(), ErrorCode::InvalidArgument);
}

TEST_F(ResultValueTest, ErrFactoryWithCodeAndMessage) {
  auto r = Result<int>::err(ErrorCode::ExecutionTimeout, "took too long");
  EXPECT_FALSE(r);
  EXPECT_EQ(r.errorCode(), ErrorCode::ExecutionTimeout);
  EXPECT_NE(r.errorMessage().find("took too long"), std::string::npos);
}

TEST_F(ResultValueTest, ErrFactoryWithNodeName) {
  auto r = Result<int>::err(ErrorCode::NodeException, "segfault", "bad_node");
  EXPECT_FALSE(r);
  EXPECT_EQ(r.error().nodeName(), "bad_node");
}

TEST_F(ResultValueTest, ValueOr) {
  Result<int> ok_r(42);
  EXPECT_EQ(ok_r.valueOr(0), 42);

  auto err_r = Result<int>::err(ErrorCode::InternalError);
  EXPECT_EQ(err_r.valueOr(-1), -1);
}

TEST_F(ResultValueTest, ValueOrRvalue) {
  auto r = Result<std::string>::ok("content");
  std::string val = std::move(r).valueOr("default");
  EXPECT_EQ(val, "content");

  auto err_r = Result<std::string>::err(ErrorCode::InternalError);
  std::string val2 = std::move(err_r).valueOr("fallback");
  EXPECT_EQ(val2, "fallback");
}

TEST_F(ResultValueTest, CopySemantics) {
  Result<int> r1(99);
  Result<int> r2 = r1;
  EXPECT_TRUE(r1);
  EXPECT_TRUE(r2);
  EXPECT_EQ(r1.value(), 99);
  EXPECT_EQ(r2.value(), 99);

  auto err = Result<int>::err(ErrorCode::InvalidState, "bad");
  Result<int> err2 = err;
  EXPECT_FALSE(err);
  EXPECT_FALSE(err2);
  EXPECT_EQ(err2.errorCode(), ErrorCode::InvalidState);
}

TEST_F(ResultValueTest, MoveSemantics) {
  auto r1 = Result<std::string>::ok("moveable");
  auto r2 = std::move(r1);
  EXPECT_TRUE(r2);
  EXPECT_EQ(r2.value(), "moveable");

  auto err1 = Result<std::string>::err(ErrorCode::ExecutionFailed, "fail");
  auto err2 = std::move(err1);
  EXPECT_FALSE(err2);
  EXPECT_EQ(err2.errorCode(), ErrorCode::ExecutionFailed);
}

TEST_F(ResultValueTest, CopyAssignment) {
  Result<int> r1(10);
  auto r2 = Result<int>::err(ErrorCode::InternalError);

  // Assign ok over err
  r2 = r1;
  EXPECT_TRUE(r2);
  EXPECT_EQ(r2.value(), 10);

  // Assign err over ok
  auto r3 = Result<int>::err(ErrorCode::InvalidArgument, "oops");
  r1 = r3;
  EXPECT_FALSE(r1);
  EXPECT_EQ(r1.errorCode(), ErrorCode::InvalidArgument);
}

TEST_F(ResultValueTest, MoveAssignment) {
  Result<int> r1(100);
  Result<int> r2(200);
  r1 = std::move(r2);
  EXPECT_TRUE(r1);
  EXPECT_EQ(r1.value(), 200);
}

TEST_F(ResultValueTest, ErrorMessage) {
  auto ok = Result<int>::ok(1);
  EXPECT_TRUE(ok.errorMessage().empty());

  auto err = Result<int>::err(ErrorCode::NodeException, "crash", "nodeX");
  std::string msg = err.errorMessage();
  EXPECT_NE(msg.find("NodeException"), std::string::npos);
  EXPECT_NE(msg.find("crash"), std::string::npos);
  EXPECT_NE(msg.find("nodeX"), std::string::npos);
}

TEST_F(ResultValueTest, ImplicitConversionFromValue) {
  // Should work: implicit construction from T
  auto fn = []() -> Result<int> { return 42; };
  auto r = fn();
  EXPECT_TRUE(r);
  EXPECT_EQ(r.value(), 42);
}

TEST_F(ResultValueTest, ImplicitConversionFromError) {
  // Should work: implicit construction from Error
  auto fn = []() -> Result<int> {
    return Error(ErrorCode::InvalidArgument, "bad");
  };
  auto r = fn();
  EXPECT_FALSE(r);
  EXPECT_EQ(r.errorCode(), ErrorCode::InvalidArgument);
}

// Test with a move-only type
TEST_F(ResultValueTest, MoveOnlyType) {
  auto r = Result<std::unique_ptr<int>>::ok(std::make_unique<int>(42));
  EXPECT_TRUE(r);
  EXPECT_EQ(*r.value(), 42);

  auto r2 = std::move(r);
  EXPECT_TRUE(r2);
  EXPECT_EQ(*r2.value(), 42);
}

class ResultVoidTest : public ::testing::Test {};

TEST_F(ResultVoidTest, SuccessConstruction) {
  Result<void> r;
  EXPECT_TRUE(r.isOk());
  EXPECT_FALSE(r.isErr());
  EXPECT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.errorCode(), ErrorCode::Ok);
}

TEST_F(ResultVoidTest, OkFactory) {
  auto r = Result<void>::ok();
  EXPECT_TRUE(r);
}

TEST_F(ResultVoidTest, ErrorConstruction) {
  Error err(ErrorCode::AlreadyRunning, "engine busy");
  Result<void> r(err);
  EXPECT_FALSE(r);
  EXPECT_EQ(r.error().code(), ErrorCode::AlreadyRunning);
  EXPECT_EQ(r.error().message(), "engine busy");
}

TEST_F(ResultVoidTest, ErrFactory) {
  auto r = Result<void>::err(ErrorCode::NotInitialized, "call init first");
  EXPECT_FALSE(r);
  EXPECT_EQ(r.errorCode(), ErrorCode::NotInitialized);
  EXPECT_NE(r.errorMessage().find("call init first"), std::string::npos);
}

TEST_F(ResultVoidTest, ErrFactoryWithNode) {
  auto r = Result<void>::err(ErrorCode::NodeException, "seg", "nodeZ");
  EXPECT_FALSE(r);
  EXPECT_EQ(r.error().nodeName(), "nodeZ");
}

TEST_F(ResultVoidTest, VoidResultAlias) {
  VoidResult r;
  EXPECT_TRUE(r);
  VoidResult err = Result<void>::err(ErrorCode::InternalError);
  EXPECT_FALSE(err);
}

TEST_F(ResultVoidTest, ImplicitConversionFromError) {
  auto fn = []() -> Result<void> {
    return Error(ErrorCode::InvalidState, "bad state");
  };
  auto r = fn();
  EXPECT_FALSE(r);
  EXPECT_EQ(r.errorCode(), ErrorCode::InvalidState);
}

TEST_F(ResultVoidTest, SuccessReturnPattern) {
  auto fn = []() -> Result<void> { return Result<void>::ok(); };
  EXPECT_TRUE(fn());
}

class PushStatusTest : public ::testing::Test {};

TEST_F(PushStatusTest, DefaultIsEnqueued) {
  PushStatus ps;
  EXPECT_EQ(ps.outcome, PushStatus::Outcome::Enqueued);
  EXPECT_FALSE(ps.isDropped());
}

TEST_F(PushStatusTest, EnqueuedFactory) {
  auto ps = PushStatus::enqueued(5);
  EXPECT_EQ(ps.outcome, PushStatus::Outcome::Enqueued);
  EXPECT_FALSE(ps.isDropped());
  EXPECT_EQ(ps.queue_size, 5u);
  EXPECT_EQ(ps.detail, "success");
}

TEST_F(PushStatusTest, DroppedFactory) {
  auto ps = PushStatus::dropped("queue full, oldest evicted", 16);
  EXPECT_EQ(ps.outcome, PushStatus::Outcome::Dropped);
  EXPECT_TRUE(ps.isDropped());
  EXPECT_EQ(ps.queue_size, 16u);
  EXPECT_NE(ps.detail.find("evicted"), std::string::npos);
}

TEST_F(PushStatusTest, InsideResultOk) {
  Result<PushStatus> r = PushStatus::enqueued(3);
  EXPECT_TRUE(r);
  EXPECT_FALSE(r.value().isDropped());
  EXPECT_EQ(r.value().queue_size, 3u);
}

TEST_F(PushStatusTest, InsideResultError) {
  auto r = Result<PushStatus>::err(ErrorCode::NodeNotFound, "no such node");
  EXPECT_FALSE(r);
  EXPECT_EQ(r.errorCode(), ErrorCode::NodeNotFound);
}

class ConfigTest : public ::testing::Test {};

TEST_F(ConfigTest, EngineConfigBatch) {
  auto cfg = EngineConfig::batch(8);
  EXPECT_EQ(cfg.mode, ExecutionMode::BATCH);
  EXPECT_EQ(cfg.num_workers, 8);
  EXPECT_EQ(cfg.default_queue_capacity, 0u);
  EXPECT_FALSE(cfg.enable_sync_coordination);
}

TEST_F(ConfigTest, EngineConfigStream) {
  auto cfg = EngineConfig::stream(4, 32);
  EXPECT_EQ(cfg.mode, ExecutionMode::STREAM);
  EXPECT_EQ(cfg.num_workers, 4);
  EXPECT_EQ(cfg.default_queue_capacity, 32u);
  EXPECT_TRUE(cfg.enable_sync_coordination);
}

TEST_F(ConfigTest, PipelineOptionsBatch) {
  auto opts = PipelineOptions::batch(6);
  EXPECT_EQ(opts.mode, ExecutionMode::BATCH);
  EXPECT_EQ(opts.num_workers, 6);
  EXPECT_EQ(opts.queue_capacity, 0u);
  EXPECT_FALSE(opts.enable_sync_coordination);
}

TEST_F(ConfigTest, PipelineOptionsStream) {
  auto opts = PipelineOptions::stream(4, 24);
  EXPECT_EQ(opts.mode, ExecutionMode::STREAM);
  EXPECT_EQ(opts.num_workers, 4);
  EXPECT_EQ(opts.queue_capacity, 24u);
  EXPECT_TRUE(opts.enable_sync_coordination);
}

TEST_F(ConfigTest, ExecutionModeToString) {
  EXPECT_EQ(executionModeToString(ExecutionMode::BATCH), "BATCH");
  EXPECT_EQ(executionModeToString(ExecutionMode::STREAM), "STREAM");
}

class ObserverTest : public ::testing::Test {};

TEST_F(ObserverTest, CallbackObserverErrorCallback) {
  CallbackObserver obs;
  Error received_error;
  bool error_called = false;

  obs.onError([&](const Error &e) {
    error_called = true;
    received_error = e;
  });

  Error test_err(ErrorCode::NodeException, "crash in detector", "detector_v2");
  obs.onExecutionFailed(test_err);

  EXPECT_TRUE(error_called);
  EXPECT_EQ(received_error.code(), ErrorCode::NodeException);
  EXPECT_EQ(received_error.message(), "crash in detector");
  EXPECT_EQ(received_error.nodeName(), "detector_v2");
}

TEST_F(ObserverTest, CallbackObserverStartCallback) {
  CallbackObserver obs;
  bool start_called = false;
  obs.onStart([&]() { start_called = true; });
  obs.onExecutionStarted();
  EXPECT_TRUE(start_called);
}

TEST_F(ObserverTest, CallbackObserverResultCallback) {
  CallbackObserver obs;
  PortDataMap received;
  obs.onResult([&](const PortDataMap &r) { received = r; });

  PortDataMap results;
  results["out"] = std::make_shared<PortData>();
  obs.onExecutionCompleted(results);

  EXPECT_EQ(received.size(), 1u);
}

TEST_F(ObserverTest, CallbackObserverDropCallback) {
  CallbackObserver obs;
  std::string dropped_node;
  uint64_t dropped_frame = 0;
  std::string dropped_reason;

  obs.onDrop([&](const std::string &n, uint64_t f, const std::string &r) {
    dropped_node = n;
    dropped_frame = f;
    dropped_reason = r;
  });

  obs.onFrameDropped("source", 42, "queue full");
  EXPECT_EQ(dropped_node, "source");
  EXPECT_EQ(dropped_frame, 42u);
  EXPECT_EQ(dropped_reason, "queue full");
}

TEST_F(ObserverTest, DefaultObserverLegacyFallback) {
  // Custom observer that overrides the legacy method
  class LegacyObserver : public IPipelineObserver {
  public:
    std::string last_msg;
    std::string last_node;

  protected:
    void onExecutionFailedLegacy(const std::string &msg,
                                 const std::string &node) override {
      last_msg = msg;
      last_node = node;
    }
  };

  LegacyObserver obs;
  Error err(ErrorCode::ExecutionFailed, "timeout", "slow_node");
  obs.onExecutionFailed(err);

  // Default implementation delegates to legacy
  EXPECT_EQ(obs.last_msg, "timeout");
  EXPECT_EQ(obs.last_node, "slow_node");
}

TEST_F(ObserverTest, CallbackObserverChaining) {
  bool start = false, result = false, error = false;

  auto obs = CallbackObserver()
                 .onStart([&]() { start = true; })
                 .onResult([&](const PortDataMap &) { result = true; })
                 .onError([&](const Error &) { error = true; });

  obs.onExecutionStarted();
  obs.onExecutionCompleted({});
  obs.onExecutionFailed(Error(ErrorCode::InternalError));

  EXPECT_TRUE(start);
  EXPECT_TRUE(result);
  EXPECT_TRUE(error);
}

class ErrorPropagationTest : public ::testing::Test {};

// Simulate a multi-step pipeline operation with error propagation
TEST_F(ErrorPropagationTest, ChainedOperations) {
  auto step1 = []() -> Result<void> { return Result<void>::ok(); };

  auto step2 = [](bool fail) -> Result<int> {
    if (fail) {
      return Result<int>::err(ErrorCode::ExecutionFailed, "step2 failed");
    }
    return 42;
  };

  auto step3 = [](int val) -> Result<std::string> {
    return "result_" + std::to_string(val);
  };

  // Success path
  {
    auto r1 = step1();
    ASSERT_TRUE(r1);
    auto r2 = step2(false);
    ASSERT_TRUE(r2);
    auto r3 = step3(r2.value());
    ASSERT_TRUE(r3);
    EXPECT_EQ(r3.value(), "result_42");
  }

  // Failure in step2
  {
    auto r1 = step1();
    ASSERT_TRUE(r1);
    auto r2 = step2(true);
    EXPECT_FALSE(r2);
    EXPECT_EQ(r2.errorCode(), ErrorCode::ExecutionFailed);
    // Step3 never called
  }
}

TEST_F(ErrorPropagationTest, ValidateAndExecutePattern) {
  // Simulate the validateState() -> execute() pattern from PipelineImpl
  enum class State { IDLE, RUNNING, ERROR };

  auto validate_state = [](State s) -> Result<void> {
    switch (s) {
    case State::IDLE:
      return Result<void>::ok();
    case State::RUNNING:
      return Error::alreadyRunning();
    case State::ERROR:
      return Error::invalidState("pipeline in error state, call reset()");
    }
    return Result<void>::err(ErrorCode::InternalError);
  };

  auto execute = [&](State s) -> Result<ExecutionOutput> {
    auto validation = validate_state(s);
    if (!validation) {
      return validation.error(); // Propagate error
    }
    ExecutionOutput out;
    out.elapsed = std::chrono::milliseconds(100);
    return out;
  };

  // IDLE -> success
  {
    auto r = execute(State::IDLE);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().elapsed.count(), 100);
  }

  // RUNNING -> error
  {
    auto r = execute(State::RUNNING);
    EXPECT_FALSE(r);
    EXPECT_EQ(r.errorCode(), ErrorCode::AlreadyRunning);
  }

  // ERROR -> error
  {
    auto r = execute(State::ERROR);
    EXPECT_FALSE(r);
    EXPECT_EQ(r.errorCode(), ErrorCode::InvalidState);
    EXPECT_NE(r.errorMessage().find("reset()"), std::string::npos);
  }
}

TEST_F(ErrorPropagationTest, PushInputPattern) {
  // Simulate the pushInput() -> Result<PushStatus> pattern
  auto push_input = [](bool streaming, bool node_found,
                       bool queue_full) -> Result<PushStatus> {
    if (!streaming) {
      return Result<PushStatus>::err(ErrorCode::NotStreaming,
                                     "engine is not in streaming mode");
    }
    if (!node_found) {
      return Result<PushStatus>::err(Error::nodeNotFound("missing"));
    }
    if (queue_full) {
      return PushStatus::dropped("oldest frame evicted", 16);
    }
    return PushStatus::enqueued(5);
  };

  // Normal enqueue
  {
    auto r = push_input(true, true, false);
    EXPECT_TRUE(r);
    EXPECT_FALSE(r.value().isDropped());
    EXPECT_EQ(r.value().queue_size, 5u);
  }

  // Dropped due to backpressure
  {
    auto r = push_input(true, true, true);
    EXPECT_TRUE(r); // Still success, just with drop info
    EXPECT_TRUE(r.value().isDropped());
    EXPECT_EQ(r.value().queue_size, 16u);
  }

  // Not streaming
  {
    auto r = push_input(false, true, false);
    EXPECT_FALSE(r);
    EXPECT_EQ(r.errorCode(), ErrorCode::NotStreaming);
  }

  // Node not found
  {
    auto r = push_input(true, false, false);
    EXPECT_FALSE(r);
    EXPECT_EQ(r.errorCode(), ErrorCode::NodeNotFound);
  }
}

TEST_F(ErrorPropagationTest, AsyncFuturePattern) {
  // Simulate runAsync() returning future<Result<ExecutionOutput>>
  auto run_async =
      [](bool should_fail) -> std::future<Result<ExecutionOutput>> {
    return std::async(std::launch::async, [should_fail]() {
      if (should_fail) {
        return Result<ExecutionOutput>::err(ErrorCode::ExecutionFailed,
                                            "node crashed", "detector");
      }
      ExecutionOutput out;
      out.outputs["result"] = std::make_shared<PortData>();
      out.elapsed = std::chrono::milliseconds(200);
      return Result<ExecutionOutput>::ok(std::move(out));
    });
  };

  // Success
  {
    auto future = run_async(false);
    auto result = future.get();
    EXPECT_TRUE(result);
    EXPECT_EQ(result.value().outputs.size(), 1u);
    EXPECT_EQ(result.value().elapsed.count(), 200);
  }

  // Failure
  {
    auto future = run_async(true);
    auto result = future.get();
    EXPECT_FALSE(result);
    EXPECT_EQ(result.errorCode(), ErrorCode::ExecutionFailed);
    EXPECT_EQ(result.error().nodeName(), "detector");
  }
}

class EdgeCaseTest : public ::testing::Test {};

TEST_F(EdgeCaseTest, EmptyStringError) {
  Error e(ErrorCode::InternalError, "", "");
  EXPECT_FALSE(e.isOk());
  EXPECT_TRUE(e.message().empty());
  EXPECT_TRUE(e.nodeName().empty());
  EXPECT_EQ(e.toString(), "InternalError");
}

TEST_F(EdgeCaseTest, LongErrorMessage) {
  std::string long_msg(1000, 'X');
  Error e(ErrorCode::ExecutionFailed, long_msg);
  EXPECT_EQ(e.message().size(), 1000u);
}

TEST_F(EdgeCaseTest, ResultSelfAssignment) {
  Result<int> r(42);
  r = r; // Self-assignment should be safe
  EXPECT_TRUE(r);
  EXPECT_EQ(r.value(), 42);
}

TEST_F(EdgeCaseTest, ResultSelfMoveAssignment) {
  Result<int> r(42);
  r = std::move(r); // Self-move-assignment
  // Behavior is implementation-defined, just shouldn't crash
}

TEST_F(EdgeCaseTest, VoidResultSelfAssignment) {
  Result<void> r;
  r = r;
  EXPECT_TRUE(r);
}

TEST_F(EdgeCaseTest, ResultWithVector) {
  auto r = Result<std::vector<int>>::ok({1, 2, 3, 4, 5});
  EXPECT_TRUE(r);
  EXPECT_EQ(r.value().size(), 5u);
  EXPECT_EQ(r.value()[2], 3);
}

TEST_F(EdgeCaseTest, MultipleErrorCodesDistinct) {
  // Ensure all error codes are distinct
  std::vector<ErrorCode> codes = {
      ErrorCode::Ok,
      ErrorCode::InternalError,
      ErrorCode::InvalidArgument,
      ErrorCode::InvalidState,
      ErrorCode::NotInitialized,
      ErrorCode::AlreadyInitialized,
      ErrorCode::GraphCycleDetected,
      ErrorCode::GraphEmpty,
      ErrorCode::InvalidConfiguration,
      ErrorCode::AlreadyRunning,
      ErrorCode::ExecutionFailed,
      ErrorCode::ExecutionTimeout,
      ErrorCode::ExecutionStopped,
      ErrorCode::ExecutionAborted,
      ErrorCode::StreamingNotSupported,
      ErrorCode::NotStreaming,
      ErrorCode::QueueRejected,
      ErrorCode::QueueFull,
      ErrorCode::NodeNotFound,
      ErrorCode::PortNotFound,
      ErrorCode::NoDownstreamConnection,
      ErrorCode::NodeException,
      ErrorCode::NodeUnknownException,
      ErrorCode::InputUnavailable,
  };

  for (size_t i = 0; i < codes.size(); ++i) {
    for (size_t j = i + 1; j < codes.size(); ++j) {
      EXPECT_NE(static_cast<uint16_t>(codes[i]),
                static_cast<uint16_t>(codes[j]))
          << "Codes at index " << i << " and " << j
          << " are equal: " << errorCodeToString(codes[i]);
    }
  }
}

// =============================================================================
// 13. Compilation Verification — include all refactored headers
// =============================================================================

// Verify that all new types are usable from a single TU
TEST_F(EdgeCaseTest, AllTypesCompile) {
  // error.hpp types
  [[maybe_unused]] ErrorCode ec = ErrorCode::Ok;
  [[maybe_unused]] Error err;
  [[maybe_unused]] Result<int> ri(0);
  [[maybe_unused]] Result<void> rv;
  [[maybe_unused]] VoidResult vr;

  // execution_types.hpp types
  [[maybe_unused]] ExecutionMode em = ExecutionMode::BATCH;
  [[maybe_unused]] QueueConfig qc;
  [[maybe_unused]] PushStatus ps;
  [[maybe_unused]] EngineConfig cfg;
  [[maybe_unused]] LatencyHistogram hist;
  [[maybe_unused]] NodeStatistics ns;

  // pipeline.hpp types
  [[maybe_unused]] PipelineOptions po;
  [[maybe_unused]] ExecutionOutput eo;
  [[maybe_unused]] CallbackObserver co;

  SUCCEED();
}
