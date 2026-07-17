/**
 * @file plugin_loader.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief PluginLoader implementation (F8)
 *
 * dlopen runs the plugin's static initializers, which is where
 * AI_PIPE_REGISTER_NODE registrations happen - i.e. registration
 * precedes the handshake by construction. The loader therefore
 * snapshots the registry around dlopen: the delta identifies the
 * plugin's node types, and on a failed handshake the delta is rolled
 * back before the library is closed (leaving a dangling factory whose
 * code is unmapped would be worse than the transient registration).
 *
 * @copyright Copyright (c) 2026
 */

#include "ai_pipe/node_registry.hpp"
#include "ai_pipe/plugin.hpp"
#include "ai_pipe/version.hpp"
#include "logger.hpp"

#include <algorithm>
#include <filesystem>
#include <unordered_set>

#ifndef _WIN32
#include <dlfcn.h>
#endif

namespace ai_pipe {

struct PluginLoader::Impl {
  std::vector<LoadedPlugin> plugins;
  std::vector<void *> handles; // parallel to plugins
};

PluginLoader::PluginLoader() : m_impl(std::make_unique<Impl>()) {}

PluginLoader::PluginLoader(PluginLoader &&) noexcept = default;
PluginLoader &PluginLoader::operator=(PluginLoader &&) noexcept = default;

PluginLoader::~PluginLoader() {
#ifndef _WIN32
  if (!m_impl) {
    return;
  }
  // Reverse load order: later plugins may depend on earlier ones.
  for (std::size_t i = m_impl->plugins.size(); i-- > 0;) {
    for (const auto &type : m_impl->plugins[i].registered_types) {
      NodeRegistry::instance().unregisterType(type);
    }
    if (m_impl->handles[i]) {
      dlclose(m_impl->handles[i]);
    }
  }
#endif
}

#ifdef _WIN32

Result<PluginLoader::LoadedPlugin>
PluginLoader::load(const std::string & /*path*/) {
  return Result<LoadedPlugin>::err(ErrorCode::PluginLoadFailed,
                                   "Plugin loading requires dlopen (POSIX)");
}

Result<std::vector<PluginLoader::LoadedPlugin>>
PluginLoader::loadDirectory(const std::string & /*directory*/) {
  return Result<std::vector<LoadedPlugin>>::err(
      ErrorCode::PluginLoadFailed, "Plugin loading requires dlopen (POSIX)");
}

Result<void> PluginLoader::unload(const std::string & /*path*/) {
  return Result<void>::err(ErrorCode::PluginLoadFailed,
                           "Plugin loading requires dlopen (POSIX)");
}

#else // POSIX

namespace {

/** @brief Pre-1.0 compatibility rule: major and minor must match */
bool frameworkVersionCompatible(std::uint32_t plugin_version) {
  constexpr std::uint32_t k_host = AI_PIPE_VERSION;
  return (plugin_version / 100) == (k_host / 100);
}

std::string versionToString(std::uint32_t version) {
  return std::to_string(version / 10000) + "." +
         std::to_string((version / 100) % 100) + "." +
         std::to_string(version % 100);
}

} // namespace

Result<PluginLoader::LoadedPlugin> PluginLoader::load(const std::string &path) {
  for (const auto &loaded : m_impl->plugins) {
    if (loaded.path == path) {
      return Result<LoadedPlugin>::err(ErrorCode::InvalidArgument,
                                       "Plugin already loaded: " + path);
    }
  }

  // Snapshot before dlopen: static initializers register node types.
  const auto types_before = NodeRegistry::instance().registeredTypes();
  const std::unordered_set<std::string> before_set(types_before.begin(),
                                                   types_before.end());

  // PluginLoader is documented single-threaded (startup-time loading),
  // and glibc's dlerror state is thread-local anyway.
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  ::dlerror(); // Clear any stale error
  void *handle = ::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char *reason = ::dlerror();
    return Result<LoadedPlugin>::err(ErrorCode::PluginLoadFailed,
                                     "dlopen failed for '" + path +
                                         "': " + (reason ? reason : "unknown"));
  }

  const auto types_after = NodeRegistry::instance().registeredTypes();
  std::vector<std::string> delta;
  for (const auto &type : types_after) {
    if (before_set.find(type) == before_set.end()) {
      delta.push_back(type);
    }
  }
  std::sort(delta.begin(), delta.end());

  auto reject = [&](ErrorCode code, const std::string &message) {
    for (const auto &type : delta) {
      NodeRegistry::instance().unregisterType(type);
    }
    ::dlclose(handle);
    return Result<LoadedPlugin>::err(code, message);
  };

  auto entry = reinterpret_cast<PluginDescriptorFn>(
      ::dlsym(handle, k_plugin_entry_symbol));
  if (!entry) {
    return reject(ErrorCode::PluginSymbolMissing,
                  "'" + path + "' is not an AI Pipe plugin (missing " +
                      std::string(k_plugin_entry_symbol) + ")");
  }

  const PluginDescriptor *descriptor = entry();
  if (!descriptor) {
    return reject(ErrorCode::PluginSymbolMissing,
                  "'" + path + "' returned a null plugin descriptor");
  }
  if (descriptor->abi_version != k_plugin_abi_version) {
    return reject(ErrorCode::PluginVersionMismatch,
                  "'" + path + "' uses plugin ABI revision " +
                      std::to_string(descriptor->abi_version) +
                      ", host expects " + std::to_string(k_plugin_abi_version));
  }
  if (!frameworkVersionCompatible(descriptor->ai_pipe_version)) {
    return reject(ErrorCode::PluginVersionMismatch,
                  "'" + path + "' was built against ai_pipe " +
                      versionToString(descriptor->ai_pipe_version) +
                      ", host is " + versionToString(AI_PIPE_VERSION) +
                      " (major.minor must match)");
  }

  LoadedPlugin loaded;
  loaded.path = path;
  loaded.name = descriptor->name ? descriptor->name : "";
  loaded.ai_pipe_version = descriptor->ai_pipe_version;
  loaded.registered_types = std::move(delta);

  LOG_INFO_S << "PluginLoader: Loaded plugin '" << loaded.name << "' from "
             << path << " (" << loaded.registered_types.size()
             << " node types)";

  m_impl->handles.push_back(handle);
  m_impl->plugins.push_back(loaded);
  return loaded;
}

Result<std::vector<PluginLoader::LoadedPlugin>>
PluginLoader::loadDirectory(const std::string &directory) {
  namespace fs = std::filesystem;

  std::error_code ec;
  if (!fs::is_directory(directory, ec)) {
    return Result<std::vector<LoadedPlugin>>::err(
        ErrorCode::InvalidArgument, "Not a directory: " + directory);
  }

  // Deterministic load order regardless of directory iteration order
  std::vector<std::string> candidates;
  for (const auto &entry : fs::directory_iterator(directory, ec)) {
    if (entry.is_regular_file() && entry.path().extension() == ".so") {
      candidates.push_back(entry.path().string());
    }
  }
  if (ec) {
    return Result<std::vector<LoadedPlugin>>::err(
        ErrorCode::PluginLoadFailed,
        "Failed scanning '" + directory + "': " + ec.message());
  }
  std::sort(candidates.begin(), candidates.end());

  std::vector<LoadedPlugin> loaded;
  loaded.reserve(candidates.size());
  for (const auto &candidate : candidates) {
    auto result = load(candidate);
    if (!result) {
      return Result<std::vector<LoadedPlugin>>::err(result.error());
    }
    loaded.push_back(std::move(result.value()));
  }
  return loaded;
}

Result<void> PluginLoader::unload(const std::string &path) {
  for (std::size_t i = 0; i < m_impl->plugins.size(); ++i) {
    if (m_impl->plugins[i].path != path) {
      continue;
    }
    for (const auto &type : m_impl->plugins[i].registered_types) {
      NodeRegistry::instance().unregisterType(type);
    }
    if (m_impl->handles[i]) {
      ::dlclose(m_impl->handles[i]);
    }
    m_impl->plugins.erase(m_impl->plugins.begin() +
                          static_cast<std::ptrdiff_t>(i));
    m_impl->handles.erase(m_impl->handles.begin() +
                          static_cast<std::ptrdiff_t>(i));
    return Result<void>::ok();
  }
  return Result<void>::err(ErrorCode::InvalidArgument,
                           "Plugin not loaded: " + path);
}

#endif // _WIN32

const std::vector<PluginLoader::LoadedPlugin> &PluginLoader::plugins() const {
  return m_impl->plugins;
}

} // namespace ai_pipe
