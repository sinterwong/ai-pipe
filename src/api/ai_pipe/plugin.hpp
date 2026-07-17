/**
 * @file plugin.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Dynamic node plugin loading via dlopen (F8)
 * @version 0.1
 * @date 2026-07-11
 *
 * A plugin is a shared library that registers node types with the
 * process-wide NodeRegistry. Registration itself needs no plugin
 * machinery - the AI_PIPE_REGISTER_NODE macros run at static
 * initialization, which dlopen triggers. What this header adds is the
 * ABI boundary and version handshake around that mechanism:
 *
 * - The plugin declares itself with AI_PIPE_PLUGIN("name"), which
 *   exports a C-linkage descriptor symbol (k_plugin_entry_symbol)
 *   carrying the plugin protocol revision and the AI_PIPE_VERSION it
 *   was built against.
 * - PluginLoader::load() dlopens the library, verifies the descriptor
 *   (rejecting foreign .so files and version mismatches - rolling back
 *   any node types the mismatched plugin already registered), and
 *   reports which node types the plugin contributed.
 *
 * ABI boundary contract: host and plugin must link the same
 * libai_pipe.so (the registry singleton and the C++ types crossing the
 * boundary - ILogicNode, PortData, Result - live there), and must be
 * built with ABI-compatible toolchains/flags. The handshake enforces
 * the framework version (pre-1.0: major and minor must match) and the
 * plugin protocol revision; it cannot detect toolchain ABI drift.
 *
 * Loaded libraries stay mapped until unload() or the PluginLoader is
 * destroyed; the caller must ensure no node instances from a plugin
 * outlive it.
 *
 * @copyright Copyright (c) 2026
 */

#ifndef AI_PIPE_PLUGIN_HPP
#define AI_PIPE_PLUGIN_HPP

#include "ai_pipe/error.hpp"
#include "ai_pipe/version.hpp"
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace ai_pipe {

// =============================================================================
// Plugin ABI
// =============================================================================

/**
 * @brief Revision of the plugin protocol itself
 *
 * Bumped when PluginDescriptor or the handshake contract changes;
 * checked for exact equality during load.
 */
inline constexpr std::uint32_t k_plugin_abi_version = 1;

/**
 * @brief C-linkage entry symbol every plugin must export
 */
inline constexpr const char *k_plugin_entry_symbol =
    "ai_pipe_plugin_descriptor";

/**
 * @brief Self-description a plugin exposes through the entry symbol
 *
 * Standard-layout, C-compatible: this struct is the only type read
 * across the dlopen boundary before the handshake passes.
 */
struct PluginDescriptor {
  std::uint32_t abi_version;     ///< Must equal k_plugin_abi_version
  std::uint32_t ai_pipe_version; ///< AI_PIPE_VERSION the plugin built against
  const char *name;              ///< Human-readable plugin name
};

/** @brief Signature of the entry symbol */
using PluginDescriptorFn = const PluginDescriptor *(*)();

// =============================================================================
// Plugin Loader
// =============================================================================

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
    std::uint32_t ai_pipe_version{0};
    /// Node types this plugin registered (registry delta during load)
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
   *   - PluginLoadFailed: dlopen failed (message carries dlerror())
   *   - PluginSymbolMissing: not an ai_pipe plugin (no descriptor)
   *   - PluginVersionMismatch: protocol or framework version mismatch;
   *     node types the plugin registered during dlopen are rolled back
   *   - InvalidArgument: already loaded from this path
   */
  Result<LoadedPlugin> load(const std::string &path);

  /**
   * @brief Scan a directory (non-recursive) and load every plugin
   *
   * Only regular files with the platform shared-library suffix (.so)
   * are considered. Fails on the first plugin that does not load;
   * plugins loaded before the failure stay loaded.
   */
  Result<std::vector<LoadedPlugin>> loadDirectory(const std::string &directory);

  /**
   * @brief Unload a plugin: unregister its node types and dlclose
   *
   * The caller must guarantee that no node instances created from this
   * plugin's factories are still alive - their code is unmapped.
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
 *   AI_PIPE_REGISTER_NODE(MyDetectorNode);
 * @endcode
 */
#define AI_PIPE_PLUGIN(plugin_name)                                            \
  extern "C" const ::ai_pipe::PluginDescriptor *ai_pipe_plugin_descriptor() {  \
    static const ::ai_pipe::PluginDescriptor descriptor{                       \
        ::ai_pipe::k_plugin_abi_version,                                       \
        static_cast<std::uint32_t>(AI_PIPE_VERSION), plugin_name};             \
    return &descriptor;                                                        \
  }

#endif // AI_PIPE_PLUGIN_HPP
