/**
 * @file context.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __PIPE_PIPELINE_CONTEXT_HPP__
#define __PIPE_PIPELINE_CONTEXT_HPP__

#include <any>
#include <memory>
#include <string>
#include <unordered_map>

namespace ai_pipe {
class PipelineContext : public std::enable_shared_from_this<PipelineContext> {
public:
  PipelineContext() = default;
  ~PipelineContext() = default;

  PipelineContext(const PipelineContext &) = delete;
  PipelineContext &operator=(const PipelineContext &) = delete;
  PipelineContext(PipelineContext &&) = default;
  PipelineContext &operator=(PipelineContext &&) = default;

  template <typename T>
  void setResource(const std::string &name, std::shared_ptr<T> resource) {
    mResources[name] = resource;
  }

  template <typename T>
  std::shared_ptr<T> getResource(const std::string &name) const {
    auto it = mResources.find(name);
    if (it == mResources.end()) {
      return nullptr;
    }
    try {
      return std::any_cast<std::shared_ptr<T>>(it->second);
    } catch (const std::bad_any_cast &) {
      return nullptr;
    }
  }

  bool hasResource(const std::string &name) const {
    return mResources.count(name) > 0;
  }

private:
  std::unordered_map<std::string, std::any> mResources;
  // std::mutex mutex_;
};
} // namespace ai_pipe

#endif
