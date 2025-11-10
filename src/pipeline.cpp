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
Pipeline::Pipeline() : m_pImpl(std::make_unique<Impl>()) {
  LOG_INFOS << "Pipeline default constructed.";
}

Pipeline::~Pipeline() { LOG_INFOS << "Pipeline destructed."; }

Pipeline::Pipeline(Pipeline &&other) noexcept
    : m_pImpl(std::move(other.m_pImpl)) {
  LOG_INFOS << "Pipeline move constructed.";
}

Pipeline &Pipeline::operator=(Pipeline &&other) noexcept {
  if (this != &other) {
    m_pImpl = std::move(other.m_pImpl);
  }
  LOG_INFOS << "Pipeline move assigned.";
  return *this;
}

bool Pipeline::initialize(Graph &&graph,
                          std::shared_ptr<PipelineContext> context,
                          const PipelineConfig &config) {
  return m_pImpl->initialize(std::move(graph), context, config);
}

bool Pipeline::initialize(Graph &&graph,
                          std::shared_ptr<PipelineContext> context,
                          uint8_t numWorkers) {
  PipelineConfig config;
  config.numWorkers = numWorkers;
  return m_pImpl->initialize(std::move(graph), context, config);
}

bool Pipeline::start() { return m_pImpl->start(); }

bool Pipeline::stop() { return m_pImpl->stop(); }

void Pipeline::reset() { m_pImpl->reset(); }

bool Pipeline::feedDataAsync(const PortDataMap &initialInputs) {
  return m_pImpl->feedDataAsync(initialInputs);
}

std::future<bool>
Pipeline::feedDataAndGetResultFuture(const PortDataMap &initialInputs) {
  return m_pImpl->feedDataAndGetResultFuture(initialInputs);
}

PipelineState Pipeline::getState() const { return m_pImpl->getState(); }

EngineState Pipeline::getEngineState() const {
  return m_pImpl->getEngineState();
}

std::unordered_map<std::string, NodeExecutionState>
Pipeline::getNodeStates() const {
  return m_pImpl->getNodeStates();
}

void Pipeline::setPipelineResultCallback(
    std::function<void(const PortDataMap &finalResults)> callback) {
  m_pImpl->setPipelineResultCallback(std::move(callback));
}

void Pipeline::setPipelineErrorCallback(
    std::function<void(const std::string &errorMsg,
                       const std::string &nodeName)>
        callback) {
  m_pImpl->setPipelineErrorCallback(std::move(callback));
}

const Graph &Pipeline::getGraph() const { return m_pImpl->getGraph(); }

PipelineContext &Pipeline::getContext() { return m_pImpl->getContext(); }

} // namespace ai_pipe