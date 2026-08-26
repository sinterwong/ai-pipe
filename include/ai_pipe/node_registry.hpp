#ifndef AI_PIPE_NODE_REGISTRY_HPP
#define AI_PIPE_NODE_REGISTRY_HPP

#include "ai_pipe/data_types.hpp"
#include "ai_pipe/error.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace ai_pipe {

/**
 * Process-wide mapping from node type names to factories.
 *
 * Registration, lookup, creation, and removal are thread-safe. Load or unload
 * plugins during application startup whenever possible so graph construction
 * cannot race with a disappearing type.
 */
class NodeRegistry {
public:
  /**
   * Factory signature receiving an instance name and type-erased config bag.
   *
   * Factories return Result so configuration errors surface as values
   * (e.g. a required config param missing) instead of exceptions.
   */
  using Factory = std::function<Result<std::shared_ptr<ILogicNode>>(
      const std::string &node_name, const PortData &config)>;

  /**
   * Returns the registry shared by the host and dynamically loaded plugins.
   * Plugins must link the same shared `ai_pipe` library as the host; embedding
   * a static copy creates a separate registry.
   */
  static NodeRegistry &instance();

  NodeRegistry(const NodeRegistry &) = delete;
  NodeRegistry &operator=(const NodeRegistry &) = delete;

  /**
   * Registers a factory under a unique, non-empty type name.
   * @return InvalidArgument if the name is empty, the factory is null,
   *         or the type name is already taken (first registration wins)
   */
  Result<void> registerFactory(const std::string &type_name, Factory factory) {
    if (type_name.empty() || !factory) {
      return Result<void>::err(ErrorCode::InvalidArgument,
                               "Type name and factory must be non-empty");
    }
    std::unique_lock lock(m_mutex);
    if (!m_factories.emplace(type_name, std::move(factory)).second) {
      return Result<void>::err(ErrorCode::InvalidArgument,
                               "Node type already registered: " + type_name);
    }
    return Result<void>::ok();
  }

  /**
   * Instantiates a node by registered type name.
   * @param type_name Registered type (e.g. class name)
   * @param node_name Instance name used in the graph
   * @param config Optional type-erased configuration bag
   */
  [[nodiscard]] Result<std::shared_ptr<ILogicNode>>
  create(const std::string &type_name, const std::string &node_name,
         const PortData &config = {}) const {
    Factory factory;
    {
      std::shared_lock lock(m_mutex);
      auto it = m_factories.find(type_name);
      if (it == m_factories.end()) {
        return Result<std::shared_ptr<ILogicNode>>::err(
            ErrorCode::InvalidArgument, "Unknown node type: " + type_name);
      }
      factory = it->second;
    }
    return factory(node_name, config);
  }

  [[nodiscard]] bool isRegistered(const std::string &type_name) const {
    std::shared_lock lock(m_mutex);
    return m_factories.find(type_name) != m_factories.end();
  }

  [[nodiscard]] std::vector<std::string> registeredTypes() const {
    std::shared_lock lock(m_mutex);
    std::vector<std::string> types;
    types.reserve(m_factories.size());
    for (const auto &[name, factory] : m_factories) {
      types.push_back(name);
    }
    return types;
  }

  /** Removes a registration; existing node instances remain alive. */
  void unregisterType(const std::string &type_name) {
    std::unique_lock lock(m_mutex);
    m_factories.erase(type_name);
  }

  /**
   * Atomically replaces a registered factory with a decorated factory.
   *
   * Support for PluginLoader instance tracking: after a plugin's static
   * initializers register their factories, the loader wraps each one so
   * created nodes carry a liveness token (see plugin_loader.cpp). The
   * decoration happens under the registry lock, so no create() can slip
   * between reading the original factory and installing the wrapper.
   *
   * @param wrap Receives the current factory, returns its replacement
   * @return InvalidArgument if the type is not registered or the
   *         wrapper returned a null factory (registration unchanged)
   */
  Result<void> wrapFactory(const std::string &type_name,
                           const std::function<Factory(Factory)> &wrap) {
    std::unique_lock lock(m_mutex);
    auto it = m_factories.find(type_name);
    if (it == m_factories.end()) {
      return Result<void>::err(ErrorCode::InvalidArgument,
                               "Unknown node type: " + type_name);
    }
    Factory wrapped = wrap(it->second);
    if (!wrapped) {
      return Result<void>::err(ErrorCode::InvalidArgument,
                               "Factory wrapper returned a null factory");
    }
    it->second = std::move(wrapped);
    return Result<void>::ok();
  }

private:
  NodeRegistry() = default;

  mutable std::shared_mutex m_mutex;
  std::unordered_map<std::string, Factory> m_factories;
};

namespace detail {
/** @brief Static-init helper backing the registration macros. */
struct NodeRegistrar {
  NodeRegistrar(const char *type_name, NodeRegistry::Factory factory) {
    // First registration wins; duplicates are reported by the Result
    // but cannot fail a static initializer, so they are ignored here.
    (void)NodeRegistry::instance().registerFactory(type_name,
                                                   std::move(factory));
  }
};
} // namespace detail

} // namespace ai_pipe

/**
 * @brief Register a node type constructible as NodeClass(name).
 *
 * Place at namespace scope in the node's translation unit:
 * @code
 *   AI_PIPE_REGISTER_NODE(MyDetectorNode);
 * @endcode
 */
#define AI_PIPE_REGISTER_NODE(NodeClass)                                       \
  static const ::ai_pipe::detail::NodeRegistrar                                \
      ai_pipe_node_registrar_##NodeClass(                                      \
          #NodeClass,                                                          \
          [](const std::string &node_name, const ::ai_pipe::PortData &)        \
              -> ::ai_pipe::Result<std::shared_ptr<::ai_pipe::ILogicNode>> {   \
            return std::shared_ptr<::ai_pipe::ILogicNode>(                     \
                std::make_shared<NodeClass>(node_name));                       \
          })

/**
 * @brief Register a node type constructible as NodeClass(name, config).
 */
#define AI_PIPE_REGISTER_NODE_WITH_CONFIG(NodeClass)                           \
  static const ::ai_pipe::detail::NodeRegistrar                                \
      ai_pipe_node_registrar_##NodeClass(                                      \
          #NodeClass,                                                          \
          [](const std::string &node_name, const ::ai_pipe::PortData &config)  \
              -> ::ai_pipe::Result<std::shared_ptr<::ai_pipe::ILogicNode>> {   \
            return std::shared_ptr<::ai_pipe::ILogicNode>(                     \
                std::make_shared<NodeClass>(node_name, config));               \
          })

#endif // AI_PIPE_NODE_REGISTRY_HPP
