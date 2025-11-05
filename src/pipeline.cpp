/**
 * @file pipeline.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "pipeline.hpp"
#include "pipeline_impl.hpp"
#include <logger.hpp>

namespace ai_pipe {
Pipeline::Pipeline() : pImpl_(std::make_unique<Impl>()) {
  LOG_INFOS << "Pipeline default constructed.";
}

Pipeline::~Pipeline() { LOG_INFOS << "Pipeline destructed."; }

Pipeline::Pipeline(Pipeline &&other) noexcept
    : pImpl_(std::move(other.pImpl_)) {
  LOG_INFOS << "Pipeline move constructed.";
}

Pipeline &Pipeline::operator=(Pipeline &&other) noexcept {
  if (this != &other) {
    pImpl_ = std::move(other.pImpl_);
  }
  LOG_INFOS << "Pipeline move assigned.";
  return *this;
}

bool Pipeline::initialize(Graph &&graph,
                          std::shared_ptr<PipelineContext> context,
                          const PipelineConfig &config) {
  return pImpl_->initialize(std::move(graph), context, config);
}

bool Pipeline::initialize(Graph &&graph,
                          std::shared_ptr<PipelineContext> context,
                          uint8_t numWorkers) {
  PipelineConfig config;
  config.numWorkers = numWorkers;
  return pImpl_->initialize(std::move(graph), context, config);
}

bool Pipeline::start() { return pImpl_->start(); }

bool Pipeline::stop() { return pImpl_->stop(); }

void Pipeline::reset() { pImpl_->reset(); }

bool Pipeline::feedDataAsync(const PortDataMap &initialInputs) {
  return pImpl_->feedDataAsync(initialInputs);
}

std::future<bool>
Pipeline::feedDataAndGetResultFuture(const PortDataMap &initialInputs) {
  return pImpl_->feedDataAndGetResultFuture(initialInputs);
}

PipelineState Pipeline::getState() const { return pImpl_->getState(); }

EngineState Pipeline::getEngineState() const {
  return pImpl_->getEngineState();
}

std::unordered_map<std::string, NodeExecutionState>
Pipeline::getNodeStates() const {
  return pImpl_->getNodeStates();
}

void Pipeline::setPipelineResultCallback(
    std::function<void(const PortDataMap &finalResults)> callback) {
  pImpl_->setPipelineResultCallback(std::move(callback));
}

void Pipeline::setPipelineErrorCallback(
    std::function<void(const std::string &errorMsg,
                       const std::string &nodeName)>
        callback) {
  pImpl_->setPipelineErrorCallback(std::move(callback));
}

const Graph &Pipeline::getGraph() const { return pImpl_->getGraph(); }

PipelineContext &Pipeline::getContext() { return pImpl_->getContext(); }

} // namespace ai_pipe