/**
 * @file benchmark_main.cpp
 * @brief Custom main entry point for all-in-one benchmark executable
 */

#include "ai_pipe/logger.hpp"
#include <benchmark/benchmark.h>

int main(int argc, char **argv) {
  ai_pipe::logging::LoggerConfig cfg;
  cfg.async_enabled = false;
  cfg.json_output = false;
  cfg.console_enabled = true;
  cfg.file_enabled = true;
  cfg.show_thread_id = true;
  cfg.file_path = "logs/ai-pipe-benchmark.log";
  cfg.min_level = ai_pipe::logging::LogLevel::Error;
  ai_pipe::logging::Logger::instance().configure(cfg);

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
