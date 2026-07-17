#include "logger.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace ai_pipe::logging;
using namespace std::chrono_literals;

namespace {

// Read a whole file into a string.
std::string readFile(const std::filesystem::path &path) {
  std::ifstream in(path);
  std::ostringstream oss;
  oss << in.rdbuf();
  return oss.str();
}

} // namespace

// =============================================================================
// Logger Test Fixture
//
// Logger is a process-wide singleton also used by PipelineContext bridging,
// so every test snapshots the configuration and restores it in TearDown.
// Console output is silenced for the duration of each test; entries are
// observed through callbacks and filtered by a per-test marker so that
// residual callbacks from other suites cannot interfere.
// =============================================================================

class LoggerTest : public ::testing::Test {
protected:
  void SetUp() override {
    auto &logger = Logger::instance();
    m_saved_config = logger.config();
    m_saved_level = logger.level();
    // Silence the console via configure() so that enableAsync()/configure()
    // calls inside tests (which re-apply m_config) keep it off
    LoggerConfig quiet = m_saved_config;
    quiet.console_enabled = false;
    logger.configure(quiet);
    logger.setLevel(LogLevel::Trace);
  }

  void TearDown() override {
    auto &logger = Logger::instance();
    for (auto id : m_callback_ids) {
      logger.removeCallback(id);
    }
    logger.configure(m_saved_config);
    logger.setLevel(m_saved_level);
    if (!m_temp_dir.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(m_temp_dir, ec);
    }
  }

  // Register a callback that is automatically removed in TearDown.
  Logger::CallbackId addScopedCallback(Logger::LogCallback callback) {
    auto id = Logger::instance().addCallback(std::move(callback));
    m_callback_ids.push_back(id);
    return id;
  }

  // Unique per-test directory for file-output tests, removed in TearDown.
  std::filesystem::path makeTempDir() {
    const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();
    m_temp_dir =
        std::filesystem::temp_directory_path() /
        (std::string("ai_pipe_logger_test_") + info->name() + "_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(m_temp_dir);
    return m_temp_dir;
  }

  LoggerConfig m_saved_config;
  LogLevel m_saved_level = LogLevel::Debug;
  std::vector<Logger::CallbackId> m_callback_ids;
  std::filesystem::path m_temp_dir;
};

// =============================================================================
// Level Filtering
// =============================================================================

TEST_F(LoggerTest, SetLevelAccessor) {
  auto &logger = Logger::instance();

  logger.setLevel(LogLevel::Warning);
  EXPECT_EQ(logger.level(), LogLevel::Warning);

  logger.setLevel(LogLevel::Trace);
  EXPECT_EQ(logger.level(), LogLevel::Trace);
}

TEST_F(LoggerTest, IsEnabledThreshold) {
  auto &logger = Logger::instance();
  logger.setLevel(LogLevel::Warning);

  EXPECT_FALSE(logger.isEnabled(LogLevel::Trace));
  EXPECT_FALSE(logger.isEnabled(LogLevel::Debug));
  EXPECT_FALSE(logger.isEnabled(LogLevel::Info));
  EXPECT_TRUE(logger.isEnabled(LogLevel::Warning));
  EXPECT_TRUE(logger.isEnabled(LogLevel::Error));
  EXPECT_TRUE(logger.isEnabled(LogLevel::Fatal));
}

TEST_F(LoggerTest, LevelFiltersDelivery) {
  auto &logger = Logger::instance();
  logger.setLevel(LogLevel::Warning);

  std::vector<LogLevel> delivered;
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("LevelFiltersDelivery") != std::string::npos) {
      delivered.push_back(entry.level);
    }
  });

  logger.info("LevelFiltersDelivery below threshold");
  logger.warning("LevelFiltersDelivery at threshold");
  logger.error("LevelFiltersDelivery above threshold");

  ASSERT_EQ(delivered.size(), 2u);
  EXPECT_EQ(delivered[0], LogLevel::Warning);
  EXPECT_EQ(delivered[1], LogLevel::Error);
}

TEST_F(LoggerTest, ConvenienceMethodsMapToLevels) {
  auto &logger = Logger::instance();

  std::vector<LogLevel> delivered;
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("ConvenienceMethods") != std::string::npos) {
      delivered.push_back(entry.level);
    }
  });

  logger.trace("ConvenienceMethods");
  logger.debug("ConvenienceMethods");
  logger.info("ConvenienceMethods");
  logger.warning("ConvenienceMethods");
  logger.error("ConvenienceMethods");
  logger.fatal("ConvenienceMethods");

  ASSERT_EQ(delivered.size(), 6u);
  EXPECT_EQ(delivered[0], LogLevel::Trace);
  EXPECT_EQ(delivered[1], LogLevel::Debug);
  EXPECT_EQ(delivered[2], LogLevel::Info);
  EXPECT_EQ(delivered[3], LogLevel::Warning);
  EXPECT_EQ(delivered[4], LogLevel::Error);
  EXPECT_EQ(delivered[5], LogLevel::Fatal);
}

// =============================================================================
// Callbacks
// =============================================================================

TEST_F(LoggerTest, CallbackReceivesEntryFields) {
  auto &logger = Logger::instance();

  LogEntry captured;
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("CallbackReceivesEntryFields") !=
        std::string::npos) {
      captured = entry;
    }
  });

  const SourceLocation loc{"dir/some_file.cpp", "someFunction", 42};
  logger.log(LogLevel::Warning, "CallbackReceivesEntryFields payload", loc,
             "my_category");

  EXPECT_EQ(captured.level, LogLevel::Warning);
  EXPECT_EQ(captured.message, "CallbackReceivesEntryFields payload");
  EXPECT_STREQ(captured.location.file, "dir/some_file.cpp");
  EXPECT_STREQ(captured.location.function, "someFunction");
  EXPECT_EQ(captured.location.line, 42);
  EXPECT_EQ(captured.category, "my_category");
  EXPECT_EQ(captured.thread_id, std::this_thread::get_id());
}

TEST_F(LoggerTest, RemoveCallbackStopsDelivery) {
  auto &logger = Logger::instance();

  int count = 0;
  auto id = logger.addCallback([&](const LogEntry &entry) {
    if (entry.message.find("RemoveCallbackStops") != std::string::npos) {
      ++count;
    }
  });

  logger.info("RemoveCallbackStops first");
  logger.removeCallback(id);
  logger.info("RemoveCallbackStops second");

  EXPECT_EQ(count, 1);
}

TEST_F(LoggerTest, CallbackIdsAreUnique) {
  auto id1 = addScopedCallback([](const LogEntry &) {});
  auto id2 = addScopedCallback([](const LogEntry &) {});

  EXPECT_NE(id1, id2);
}

TEST_F(LoggerTest, ClearCallbacksRemovesAll) {
  auto &logger = Logger::instance();

  int count = 0;
  logger.addCallback([&](const LogEntry &entry) {
    if (entry.message.find("ClearCallbacks") != std::string::npos) {
      ++count;
    }
  });
  logger.addCallback([&](const LogEntry &entry) {
    if (entry.message.find("ClearCallbacks") != std::string::npos) {
      ++count;
    }
  });

  logger.info("ClearCallbacks before");
  EXPECT_EQ(count, 2);

  logger.clearCallbacks();
  logger.info("ClearCallbacks after");
  EXPECT_EQ(count, 2);
}

TEST_F(LoggerTest, ThrowingCallbackIsSwallowed) {
  auto &logger = Logger::instance();

  addScopedCallback(
      [](const LogEntry &) { throw std::runtime_error("sink failure"); });

  int count = 0;
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("ThrowingCallback") != std::string::npos) {
      ++count;
    }
  });

  EXPECT_NO_THROW(logger.info("ThrowingCallback survives"));
  EXPECT_EQ(count, 1);
}

// =============================================================================
// Formatting Entry Points (logf / LogStream)
// =============================================================================

TEST_F(LoggerTest, LogfFormatsPrintfStyle) {
  auto &logger = Logger::instance();

  std::string captured;
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("LogfFormats") != std::string::npos) {
      captured = entry.message;
    }
  });

  logger.logf(LogLevel::Info, SourceLocation{}, {},
              "LogfFormats value=%d name=%s", 42, "pipe");

  EXPECT_EQ(captured, "LogfFormats value=42 name=pipe");
}

TEST_F(LoggerTest, LogfSkipsDisabledLevel) {
  auto &logger = Logger::instance();
  logger.setLevel(LogLevel::Error);

  int count = 0;
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("LogfSkips") != std::string::npos) {
      ++count;
    }
  });

  logger.logf(LogLevel::Debug, SourceLocation{}, {}, "LogfSkips %d", 1);
  EXPECT_EQ(count, 0);
}

TEST_F(LoggerTest, LogStreamAccumulatesAndLogsOnDestruction) {
  auto &logger = Logger::instance();

  std::string captured;
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("LogStreamAccumulates") != std::string::npos) {
      captured = entry.message;
    }
  });

  {
    LogStream stream(logger, LogLevel::Info, SourceLocation{});
    stream << "LogStreamAccumulates " << 7 << " " << std::hex << 255;
    EXPECT_TRUE(captured.empty()); // Not logged until destruction
  }

  EXPECT_EQ(captured, "LogStreamAccumulates 7 ff");
}

TEST_F(LoggerTest, LogStreamDisabledLevelDoesNotLog) {
  auto &logger = Logger::instance();
  logger.setLevel(LogLevel::Error);

  int count = 0;
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("LogStreamDisabled") != std::string::npos) {
      ++count;
    }
  });

  {
    LogStream(logger, LogLevel::Info, SourceLocation{}) << "LogStreamDisabled";
  }

  EXPECT_EQ(count, 0);
}

// =============================================================================
// Configuration
// =============================================================================

TEST_F(LoggerTest, ConfigureRoundTrip) {
  auto &logger = Logger::instance();

  LoggerConfig cfg = m_saved_config;
  cfg.min_level = LogLevel::Error;
  cfg.console_enabled = false;
  cfg.show_thread_id = false;
  cfg.max_backup_count = 3;
  cfg.pattern = "%L %m";
  logger.configure(cfg);

  LoggerConfig readback = logger.config();
  EXPECT_EQ(readback.min_level, LogLevel::Error);
  EXPECT_FALSE(readback.console_enabled);
  EXPECT_FALSE(readback.show_thread_id);
  EXPECT_EQ(readback.max_backup_count, 3);
  EXPECT_EQ(readback.pattern, "%L %m");
  EXPECT_EQ(logger.level(), LogLevel::Error);
}

TEST_F(LoggerTest, SetPatternRoundTrip) {
  auto &logger = Logger::instance();

  logger.setPattern("%T | %m");
  EXPECT_EQ(logger.config().pattern, "%T | %m");
}

// =============================================================================
// File Output
// =============================================================================

TEST_F(LoggerTest, FileOutputWritesFormattedLines) {
  auto &logger = Logger::instance();
  auto dir = makeTempDir();
  auto log_path = dir / "out.log";

  LoggerConfig cfg = m_saved_config;
  cfg.console_enabled = false;
  cfg.file_enabled = true;
  cfg.file_path = log_path.string();
  logger.configure(cfg);

  logger.info("FileOutput hello", SourceLocation{"a/b/file.cpp", "fn", 12},
              "file_cat");
  logger.flush();

  std::string content = readFile(log_path);
  EXPECT_NE(content.find("FileOutput hello"), std::string::npos);
  EXPECT_NE(content.find("INFO"), std::string::npos);
  EXPECT_NE(content.find("[file_cat]"), std::string::npos);
  // Source location is reduced to the basename
  EXPECT_NE(content.find("file.cpp:12"), std::string::npos);
  EXPECT_EQ(content.find("a/b/file.cpp"), std::string::npos);
  // No ANSI color codes in files
  EXPECT_EQ(content.find('\033'), std::string::npos);
}

TEST_F(LoggerTest, FileOutputCreatesParentDirectories) {
  auto &logger = Logger::instance();
  auto dir = makeTempDir();
  auto log_path = dir / "nested" / "deeper" / "out.log";

  LoggerConfig cfg = m_saved_config;
  cfg.console_enabled = false;
  cfg.file_enabled = true;
  cfg.file_path = log_path.string();
  logger.configure(cfg);

  logger.info("FileOutputNested hello");
  logger.flush();

  EXPECT_TRUE(std::filesystem::exists(log_path));
  EXPECT_NE(readFile(log_path).find("FileOutputNested hello"),
            std::string::npos);
}

TEST_F(LoggerTest, ErrorLevelFlushesImmediately) {
  auto &logger = Logger::instance();
  auto dir = makeTempDir();
  auto log_path = dir / "out.log";

  LoggerConfig cfg = m_saved_config;
  cfg.console_enabled = false;
  cfg.file_enabled = true;
  cfg.file_path = log_path.string();
  logger.configure(cfg);

  // No explicit flush: Error/Fatal must hit the file immediately
  logger.error("ErrorFlush immediate");

  EXPECT_NE(readFile(log_path).find("ErrorFlush immediate"), std::string::npos);
}

TEST_F(LoggerTest, EnableFileToggle) {
  auto &logger = Logger::instance();
  auto dir = makeTempDir();
  auto log_path = dir / "toggle.log";

  // Point the config at the temp file while keeping file output off, then
  // exercise the enableFile() fast-toggle path
  LoggerConfig cfg = m_saved_config;
  cfg.console_enabled = false;
  cfg.file_enabled = false;
  cfg.file_path = log_path.string();
  logger.configure(cfg);

  logger.info("EnableFileToggle before");
  logger.enableFile(true);
  logger.info("EnableFileToggle during");
  logger.enableFile(false);
  logger.info("EnableFileToggle after");

  std::string content = readFile(log_path);
  EXPECT_EQ(content.find("EnableFileToggle before"), std::string::npos);
  EXPECT_NE(content.find("EnableFileToggle during"), std::string::npos);
  EXPECT_EQ(content.find("EnableFileToggle after"), std::string::npos);
}

TEST_F(LoggerTest, SetFilePathSwitchesTarget) {
  auto &logger = Logger::instance();
  auto dir = makeTempDir();
  auto first = dir / "first.log";
  auto second = dir / "second.log";

  LoggerConfig cfg = m_saved_config;
  cfg.console_enabled = false;
  cfg.file_enabled = true;
  cfg.file_path = first.string();
  logger.configure(cfg);

  logger.info("SetFilePath one");
  logger.setFilePath(second.string());
  logger.info("SetFilePath two");
  logger.flush();

  EXPECT_NE(readFile(first).find("SetFilePath one"), std::string::npos);
  EXPECT_EQ(readFile(first).find("SetFilePath two"), std::string::npos);
  EXPECT_NE(readFile(second).find("SetFilePath two"), std::string::npos);
}

TEST_F(LoggerTest, FileRotationKeepsBackupChain) {
  auto &logger = Logger::instance();
  auto dir = makeTempDir();
  auto log_path = dir / "rotate.log";

  LoggerConfig cfg = m_saved_config;
  cfg.console_enabled = false;
  cfg.file_enabled = true;
  cfg.file_path = log_path.string();
  cfg.max_file_size = 256; // Force frequent rotation
  cfg.max_backup_count = 2;
  logger.configure(cfg);

  for (int i = 0; i < 60; ++i) {
    logger.info("FileRotation padding line to exceed the threshold " +
                std::to_string(i));
  }
  logger.flush();

  EXPECT_TRUE(std::filesystem::exists(log_path));
  EXPECT_TRUE(std::filesystem::exists(log_path.string() + ".1"));
  // The chain never grows beyond max_backup_count
  EXPECT_FALSE(std::filesystem::exists(log_path.string() + ".3"));

  // Rotated-out backup stays within the size bound (plus one trailing line)
  EXPECT_LE(std::filesystem::file_size(log_path.string() + ".1"), 512u);
}

// =============================================================================
// JSON Output
// =============================================================================

TEST_F(LoggerTest, JsonOutputEscapesSpecialCharacters) {
  auto &logger = Logger::instance();
  auto dir = makeTempDir();
  auto log_path = dir / "json.log";

  LoggerConfig cfg = m_saved_config;
  cfg.console_enabled = false;
  cfg.file_enabled = true;
  cfg.file_path = log_path.string();
  cfg.json_output = true;
  logger.configure(cfg);

  logger.info("JsonEscape quote=\" backslash=\\ nl=\n tab=\t cr=\r end",
              SourceLocation{"src/x.cpp", "fx", 7}, "jcat");
  logger.flush();

  std::string content = readFile(log_path);
  ASSERT_FALSE(content.empty());

  EXPECT_NE(content.find("\"level\":\"INFO\""), std::string::npos);
  EXPECT_NE(content.find("\"ts\":\""), std::string::npos);
  EXPECT_NE(content.find("\"cat\":\"jcat\""), std::string::npos);
  EXPECT_NE(content.find("\"file\":\"x.cpp\",\"line\":7"), std::string::npos);
  EXPECT_NE(content.find("\"func\":\"fx\""), std::string::npos);
  EXPECT_NE(content.find("quote=\\\""), std::string::npos);
  EXPECT_NE(content.find("backslash=\\\\"), std::string::npos);
  EXPECT_NE(content.find("nl=\\n"), std::string::npos);
  EXPECT_NE(content.find("tab=\\t"), std::string::npos);
  EXPECT_NE(content.find("cr=\\r"), std::string::npos);
  // Raw control characters must not survive inside the JSON line
  std::string payload = content.substr(content.find("JsonEscape"));
  EXPECT_EQ(payload.find('\t'), std::string::npos);
}

// =============================================================================
// Async Mode
// =============================================================================

TEST_F(LoggerTest, AsyncModeDeliversAllEntries) {
  auto &logger = Logger::instance();

  std::atomic<int> count{0};
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("AsyncDelivers") != std::string::npos) {
      count.fetch_add(1, std::memory_order_relaxed);
    }
  });

  logger.enableAsync(true);
  constexpr int k_total = 200;
  for (int i = 0; i < k_total; ++i) {
    logger.info("AsyncDelivers " + std::to_string(i));
  }
  logger.flush();
  // Disabling async joins the worker and drains the ring buffer, so after
  // this call every entry must have been processed
  logger.enableAsync(false);

  EXPECT_EQ(count.load(), k_total);
}

TEST_F(LoggerTest, AsyncQueueFullFallsBackToSyncWithoutLoss) {
  auto &logger = Logger::instance();

  std::atomic<int> count{0};
  addScopedCallback([&](const LogEntry &entry) {
    if (entry.message.find("AsyncFallback") != std::string::npos) {
      count.fetch_add(1, std::memory_order_relaxed);
      // Stall the consumer so the tiny ring buffer fills up and the
      // producer takes the synchronous fallback path
      std::this_thread::sleep_for(1ms);
    }
  });

  LoggerConfig cfg = logger.config();
  cfg.console_enabled = false;
  cfg.async_enabled = true;
  cfg.async_queue_size = 4;
  logger.configure(cfg);

  constexpr int k_total = 50;
  for (int i = 0; i < k_total; ++i) {
    logger.info("AsyncFallback " + std::to_string(i));
  }
  logger.enableAsync(false); // Joins worker and drains remaining entries

  EXPECT_EQ(count.load(), k_total);
}

// =============================================================================
// LogEntry / SourceLocation
// =============================================================================

TEST(LogEntryTest, DefaultConstruction) {
  LogEntry entry;
  EXPECT_EQ(entry.level, LogLevel::Info);
  EXPECT_TRUE(entry.message.empty());
  EXPECT_TRUE(entry.category.empty());
}

TEST(LogEntryTest, ConstructionCapturesContext) {
  const SourceLocation loc{"f.cpp", "fn", 3};
  LogEntry entry(LogLevel::Error, "boom", loc, "cat");

  EXPECT_EQ(entry.level, LogLevel::Error);
  EXPECT_EQ(entry.message, "boom");
  EXPECT_EQ(entry.location.line, 3);
  EXPECT_EQ(entry.category, "cat");
  EXPECT_EQ(entry.thread_id, std::this_thread::get_id());
  EXPECT_LE(entry.timestamp, std::chrono::system_clock::now());
}

TEST(SourceLocationTest, DefaultIsEmpty) {
  constexpr SourceLocation loc;
  EXPECT_STREQ(loc.file, "");
  EXPECT_STREQ(loc.function, "");
  EXPECT_EQ(loc.line, 0);
}

#if AI_PIPE_HAS_SOURCE_LOCATION
TEST(SourceLocationTest, CurrentCapturesThisFile) {
  auto loc = SourceLocation::current();
  EXPECT_NE(std::string_view(loc.file).find("test_logger.cpp"),
            std::string_view::npos);
  EXPECT_GT(loc.line, 0);
}
#endif

// =============================================================================
// SPSC Ring Buffer
// =============================================================================

TEST(SPSCRingBufferTest, CapacityRoundsUpToPowerOfTwo) {
  SPSCRingBuffer<int> buffer(5);
  EXPECT_EQ(buffer.capacity(), 8u);

  SPSCRingBuffer<int> exact(16);
  EXPECT_EQ(exact.capacity(), 16u);
}

TEST(SPSCRingBufferTest, FifoOrder) {
  SPSCRingBuffer<int> buffer(8);

  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(buffer.tryPush(int(i)));
  }

  int value = -1;
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(buffer.tryPop(value));
    EXPECT_EQ(value, i);
  }
  EXPECT_TRUE(buffer.empty());
}

TEST(SPSCRingBufferTest, PopOnEmptyFails) {
  SPSCRingBuffer<int> buffer(4);
  int value = 0;

  EXPECT_TRUE(buffer.empty());
  EXPECT_FALSE(buffer.tryPop(value));
}

TEST(SPSCRingBufferTest, PushOnFullFails) {
  // One slot is sacrificed to distinguish full from empty
  SPSCRingBuffer<int> buffer(4);

  EXPECT_TRUE(buffer.tryPush(1));
  EXPECT_TRUE(buffer.tryPush(2));
  EXPECT_TRUE(buffer.tryPush(3));
  EXPECT_FALSE(buffer.tryPush(4));

  int value = 0;
  EXPECT_TRUE(buffer.tryPop(value));
  EXPECT_EQ(value, 1);
  // Space freed: push succeeds again and wraps around
  EXPECT_TRUE(buffer.tryPush(4));
  EXPECT_FALSE(buffer.tryPush(5));
}

TEST(SPSCRingBufferTest, ConcurrentProducerConsumer) {
  SPSCRingBuffer<int> buffer(64);
  constexpr int k_total = 10000;

  std::thread producer([&] {
    for (int i = 0; i < k_total; ++i) {
      while (!buffer.tryPush(int(i))) {
        std::this_thread::yield();
      }
    }
  });

  long long sum = 0;
  int received = 0;
  int value = 0;
  while (received < k_total) {
    if (buffer.tryPop(value)) {
      sum += value;
      ++received;
    } else {
      std::this_thread::yield();
    }
  }
  producer.join();

  EXPECT_EQ(sum, static_cast<long long>(k_total) * (k_total - 1) / 2);
  EXPECT_TRUE(buffer.empty());
}

// =============================================================================
// Hex Dump
// =============================================================================

TEST(HexDumpTest, FormatsPrintableAscii) {
  const char data[] = "Hello";
  std::string dump = hexDump(data, 5);

  EXPECT_EQ(dump.rfind("00000000 ", 0), 0u); // Starts with offset
  EXPECT_NE(dump.find("48 65 6C 6C 6F"), std::string::npos);
  EXPECT_NE(dump.find("|Hello|"), std::string::npos);
  EXPECT_EQ(dump.back(), '\n');
}

TEST(HexDumpTest, NonPrintableBytesBecomeDots) {
  const uint8_t data[] = {0x00, 0x1F, 'A', 0x7F, 0xFF};
  std::string dump = hexDump(data, sizeof(data));

  EXPECT_NE(dump.find("00 1F 41 7F FF"), std::string::npos);
  EXPECT_NE(dump.find("|..A..|"), std::string::npos);
}

TEST(HexDumpTest, SplitsIntoLinesByWidth) {
  std::vector<uint8_t> data(20, 0x41); // 'A' x 20
  std::string dump = hexDump(data.data(), data.size(), 16);

  // 20 bytes at 16 per line -> two lines with offsets 0 and 0x10
  EXPECT_NE(dump.find("00000000 "), std::string::npos);
  EXPECT_NE(dump.find("00000010 "), std::string::npos);
  EXPECT_EQ(std::count(dump.begin(), dump.end(), '\n'), 2);
  EXPECT_NE(dump.find("|AAAAAAAAAAAAAAAA|"), std::string::npos);
  EXPECT_NE(dump.find("|AAAA|"), std::string::npos);
}

TEST(HexDumpTest, CustomBytesPerLine) {
  std::vector<uint8_t> data(8, 0x42); // 'B' x 8
  std::string dump = hexDump(data.data(), data.size(), 4);

  EXPECT_EQ(std::count(dump.begin(), dump.end(), '\n'), 2);
  EXPECT_NE(dump.find("00000004 "), std::string::npos);
  EXPECT_NE(dump.find("|BBBB|"), std::string::npos);
}
