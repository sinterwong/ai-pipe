
/**
 * @file type_safe_factory.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-03
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_TYPE_SAFE_FACTORY_HPP
#define AI_PIPE_TYPE_SAFE_FACTORY_HPP

#include "ai_pipe/data_packet.hpp"
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>

namespace ai_pipe::common_utils {

template <class BaseClass> class Factory {
public:
  // takes a const refer to paramters and returns a shared_ptr to the BaseClass
  using Creator = std::function<std::shared_ptr<BaseClass>(const DataPacket &)>;

  static Factory &instance() {
    static Factory instance;
    return instance;
  }

  bool registerCreator(const std::string &class_name, Creator creator) {
    if (!creator) {
      throw std::runtime_error("Cannot register a null creator");
    }

    auto [it, success] =
        m_creatorRegistry.insert({class_name, std::move(creator)});
    return success;
  }

  std::shared_ptr<BaseClass> create(const std::string &class_name,
                                    const DataPacket &params = {}) const {
    auto it = m_creatorRegistry.find(class_name);
    if (it == m_creatorRegistry.end()) {
      throw std::runtime_error("Factory error: Class '" + class_name +
                               "' not registered for base type '" +
                               typeid(BaseClass).name() + "'.");
    }

    try {
      return it->second(params);
    } catch (const std::exception &e) {
      throw std::runtime_error("Factory error: Failed to create '" + class_name +
                               "': " + e.what());
    }
  }

  bool isRegistered(const std::string &class_name) const {
    return m_creatorRegistry.count(class_name);
  }

private:
  Factory() = default;
  ~Factory() = default;

  // singleton access
  Factory(const Factory &) = delete;
  Factory &operator=(const Factory &) = delete;
  Factory(Factory &&) = delete;
  Factory &operator=(Factory &&) = delete;

  std::map<std::string, Creator> m_creatorRegistry;
};

} // namespace ai_pipe::common_utils

#endif