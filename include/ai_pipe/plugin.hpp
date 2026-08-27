#ifndef AI_PIPE_PLUGIN_HPP
#define AI_PIPE_PLUGIN_HPP

#include "ai_pipe/error.hpp"
#include "ai_pipe/version.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace ai_pipe {

class NodeRegistry;

// Plugin ABI

/**
 * @brief Revision of the plugin protocol itself
 *
 * Bumped when PluginDescriptor or the handshake contract changes;
 * checked for exact equality during load.
 */
inline constexpr std::uint32_t k_plugin_abi_version = 2;

/**
 * @brief C-linkage entry symbol every plugin must export
 */
inline constexpr const char *k_plugin_entry_symbol =
    "ai_pipe_plugin_descriptor";

/** C-linkage entry invoked after the descriptor handshake succeeds. */
inline constexpr const char *k_plugin_registration_symbol =
    "ai_pipe_register_plugin_v1";

/**
 * @brief Self-description a plugin exposes through the entry symbol
 *
 * Standard-layout, C-compatible: this struct is the only type read
 * across the dynamic-library boundary before the handshake passes.
 */
struct PluginDescriptor {
  std::uint32_t abi_version;       ///< Must equal k_plugin_abi_version
  std::uint32_t ai_pipe_version;   ///< AI_PIPE_VERSION the plugin built against
  const char *name;                ///< Human-readable plugin name
  const char *version;             ///< Plugin's own semantic version
  const char *provider;            ///< Stable vendor or organization id
  const char *description;         ///< Short human-readable summary
  const char *const *capabilities; ///< Array of capability strings
  std::uint32_t capability_count;  ///< Number of capability strings
};

/** @brief Signature of the entry symbol */
using PluginDescriptorFn = const PluginDescriptor *(*)();

/** Registration entry signature. Exceptions must not cross this boundary. */
using PluginRegistrationFn = bool (*)(NodeRegistry &);

// Plugin Loader

/**
 * @brief Loads node plugins from shared libraries
 *
 * Not thread-safe: load plugins from a single thread during startup,
 * before pipelines start instantiating nodes.
 */
class PluginLoader {
public:
  /**
   * @brief A successfully loaded plugin
   */
  struct LoadedPlugin {
    std::string path;
    std::string name;
    std::string version;
    std::string provider;
    std::string description;
    std::vector<std::string> capabilities;
    std::uint32_t ai_pipe_version{0};
    /// Stable node type ids this plugin registered.
    std::vector<std::string> registered_types;
  };

  PluginLoader();
  ~PluginLoader();

  PluginLoader(const PluginLoader &) = delete;
  PluginLoader &operator=(const PluginLoader &) = delete;
  PluginLoader(PluginLoader &&) noexcept;
  PluginLoader &operator=(PluginLoader &&) noexcept;

  /**
   * @brief Load a single plugin library
   * @return LoadedPlugin on success, or Error with:
   *   - PluginLoadFailed: the platform loader or plugin initialization failed
   *   - PluginSymbolMissing: not an ai_pipe plugin (no descriptor)
   *   - PluginVersionMismatch: protocol or framework version mismatch;
   *     node types the plugin registered during dlopen are rolled back
   *   - InvalidArgument: already loaded from this path
   */
  Result<LoadedPlugin> load(const std::string &path);

  /**
   * @brief Scan a directory (non-recursive) and load every plugin
   *
   * Only regular platform shared libraries whose filename contains
   * `ai_pipe_plugin_` are considered. Fails on the first plugin that does not
   * load; plugins loaded before the failure stay loaded.
   */
  Result<std::vector<LoadedPlugin>> loadDirectory(const std::string &directory);

  /**
   * Returns the environment and installation-relative discovery paths.
   * `AI_PIPE_PLUGIN_PATH` uses ':' on POSIX and ';' on Windows.
   */
  [[nodiscard]] std::vector<std::filesystem::path> defaultSearchPaths() const;

  /**
   * Loads plugins from explicit paths followed by the default search paths.
   * Only libraries whose filename contains `ai_pipe_plugin_` are candidates.
   */
  Result<std::vector<LoadedPlugin>>
  discover(const std::vector<std::filesystem::path> &search_paths = {});

  /**
   * @brief Deactivate a plugin by unregistering its node types
   *
   * The shared library deliberately remains mapped until process exit. This
   * keeps existing node instances, cross-plugin dependencies, callbacks, and
   * static objects safe. Existing instances remain usable; new instances can no
   * longer be created through NodeRegistry.
   */
  Result<void> unload(const std::string &path);

  /** @brief Plugins currently loaded, in load order */
  [[nodiscard]] const std::vector<LoadedPlugin> &plugins() const;

private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

} // namespace ai_pipe

/**
 * @brief Declare a shared library as an AI Pipe plugin
 *
 * Place once at namespace scope in the plugin (any translation unit):
 * @code
 *   AI_PIPE_PLUGIN("my_detector_pack");
 *   extern "C" AI_PIPE_PLUGIN_EXPORT bool
 *   ai_pipe_register_plugin_v1(ai_pipe::NodeRegistry &registry) {
 *     return registry.registerNode<MyDetectorNode>("acme.detector").isOk();
 *   }
 * @endcode
 */
#if defined(_WIN32)
#define AI_PIPE_PLUGIN_EXPORT __declspec(dllexport)
#else
#define AI_PIPE_PLUGIN_EXPORT __attribute__((visibility("default")))
#endif

#define AI_PIPE_PLUGIN_WITH_METADATA(plugin_name, plugin_version,              \
                                     plugin_provider, plugin_description)      \
  extern "C" AI_PIPE_PLUGIN_EXPORT const ::ai_pipe::PluginDescriptor *         \
  ai_pipe_plugin_descriptor() {                                                \
    static const char *const capabilities[] = {"node"};                        \
    static const ::ai_pipe::PluginDescriptor descriptor{                       \
        ::ai_pipe::k_plugin_abi_version,                                       \
        static_cast<std::uint32_t>(AI_PIPE_VERSION),                           \
        plugin_name,                                                           \
        plugin_version,                                                        \
        plugin_provider,                                                       \
        plugin_description,                                                    \
        capabilities,                                                          \
        1};                                                                    \
    return &descriptor;                                                        \
  }

#define AI_PIPE_PLUGIN(plugin_name)                                            \
  AI_PIPE_PLUGIN_WITH_METADATA(plugin_name, AI_PIPE_VERSION_STR, "", "")

#endif // AI_PIPE_PLUGIN_HPP
