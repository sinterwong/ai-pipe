/**
 * @file execution_engine_factory.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Execution Engine Factory for creating and registering execution
 * engines
 * @version 0.1
 * @date 2025-11-05
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_EXECUTION_ENGINE_FACTORY_HPP
#define AI_PIPE_EXECUTION_ENGINE_FACTORY_HPP

#include "ai_pipe/data_packet.hpp"
#include "ai_pipe/i_execution_engine.hpp"
#include "ai_pipe/type_safe_factory.hpp"
#include <memory>
#include <string>

namespace ai_pipe {

// ExecutionEngine factory type alias
using ExecutionEngineFactory = common_utils::Factory<IExecutionEngine>;

// Engine creation parameters type alias
using EngineConstructParams = common_utils::DataPacket;

/**
 * @brief Macro to register an ExecutionEngine implementation
 *
 * This macro simplifies the registration of ExecutionEngine implementations
 * to the factory. It creates a static registration object that registers the
 * engine creator function at program startup.
 *
 * Usage example:
 *   AI_PIPE_REGISTER_ENGINE(DefaultExecutionEngine)
 *
 * @param EngineType The concrete ExecutionEngine class to register (without
 * namespace)
 * @param EngineClass The fully qualified class name with namespace
 */
#define AI_PIPE_REGISTER_ENGINE(EngineType, EngineClass)                       \
  namespace {                                                                  \
  [[maybe_unused]] const auto ___##EngineType##Registration__ =                \
      ai_pipe::ExecutionEngineFactory::instance().registerCreator(             \
          #EngineType,                                                         \
          [](const ai_pipe::EngineConstructParams &)                           \
              -> std::shared_ptr<ai_pipe::IExecutionEngine> {                  \
            return std::make_shared<EngineClass>();                            \
          });                                                                  \
  }

/**
 * @brief Helper function to create an ExecutionEngine instance
 *
 * @param engineType The type name of the engine to create (e.g.,
 * "DefaultExecutionEngine")
 * @param params Optional construction parameters
 * @return std::shared_ptr<IExecutionEngine> The created engine instance
 * @throws std::runtime_error if the engine type is not registered
 */
inline std::shared_ptr<IExecutionEngine>
createExecutionEngine(const std::string &engine_type = "DefaultExecutionEngine",
                      const EngineConstructParams &params = {}) {
  return ExecutionEngineFactory::instance().create(engine_type, params);
}

} // namespace ai_pipe

#endif // __AI_PIPE_EXECUTION_ENGINE_FACTORY_HPP__
