/**
 * @file ai_pipe.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Main include file for AI Pipe library
 * @version 1.0
 * @date 2025-12-24
 *
 * This file provides a single include point for all public API components
 * of the AI Pipe library.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_AI_PIPE_HPP
#define AI_PIPE_AI_PIPE_HPP

// Core Types
#include "ai_pipe/data_types.hpp"
#include "ai_pipe/enum.hpp"

// Graph and Node Interfaces
#include "ai_pipe/edge.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/node_registry.hpp"

// Pipeline Context
#include "ai_pipe/context.hpp"

// Execution Types and Configuration
#include "ai_pipe/execution_types.hpp"

// Strategy Interfaces (for custom implementations)
#include "ai_pipe/i_scheduler_strategy.hpp"
#include "ai_pipe/i_sync_strategy.hpp"

// Execution Engine
#include "ai_pipe/execution_engine.hpp"

// High-Level Pipeline API
#include "ai_pipe/pipeline.hpp"

// Frame Metadata (for streaming scenarios)
#include "ai_pipe/frame_metadata.hpp"

// Execution Tracing (per-frame span events)
#include "ai_pipe/trace.hpp"

// Dynamic Node Plugins (dlopen loading)
#include "ai_pipe/plugin.hpp"

#endif // AI_PIPE_AI_PIPE_HPP
