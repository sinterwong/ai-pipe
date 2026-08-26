#ifndef AI_PIPE_ENUM_HPP
#define AI_PIPE_ENUM_HPP

namespace ai_pipe {
/** Current state of a node execution attempt. */
enum class NodeExecutionState {
  WAITING,   ///< Waiting for required input packets.
  READY,     ///< Required inputs are available.
  EXECUTING, ///< `process()` is running.
  COMPLETED, ///< Processing completed successfully.
  FAILED     ///< Processing failed.
};

/** Lifecycle state of an execution engine. */
enum class EngineState {
  IDLE,    ///< No execution is active.
  RUNNING, ///< The engine is accepting or processing work.
  STOPPED, ///< Streaming execution has stopped.
  ERROR    ///< An unrecovered execution error occurred.
};

/** Lifecycle state exposed by the `Pipeline` facade. */
enum class PipelineState {
  UNINITIALIZED, ///< The pipeline has not been built.
  IDLE,          ///< The pipeline is ready for an execution.
  RUNNING,       ///< An execution or stream is active.
  STOPPING,      ///< Streaming shutdown is in progress.
  ERROR          ///< `reset()` is required before the next execution.
};
} // namespace ai_pipe
#endif
