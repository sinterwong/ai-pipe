#include "ai_pipe/node_registry.hpp"
#include "ai_pipe/plugin.hpp"
#include "ai_pipe/version.hpp"
#include "logger.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <unordered_set>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

namespace ai_pipe {

namespace {

namespace fs = std::filesystem;

std::string canonicalIdentity(const std::string &path) {
  std::error_code error;
  auto canonical = fs::weakly_canonical(fs::path(path), error);
  return error ? path : canonical.string();
}

bool isPluginCandidate(const fs::path &path) {
  const auto filename = path.filename().string();
  if (filename.find("ai_pipe_plugin_") == std::string::npos) {
    return false;
  }
#if defined(_WIN32)
  return path.extension() == ".dll";
#elif defined(__APPLE__)
  return filename.ends_with(".dylib") ||
         filename.find(".so") != std::string::npos;
#else
  return filename.find(".so") != std::string::npos;
#endif
}

std::vector<fs::path> splitSearchPath(const char *value) {
  std::vector<fs::path> paths;
  if (!value) {
    return paths;
  }
#if defined(_WIN32)
  constexpr char separator = ';';
#else
  constexpr char separator = ':';
#endif
  std::string source(value);
  std::size_t begin = 0;
  while (begin <= source.size()) {
    const auto end = source.find(separator, begin);
    const auto item = source.substr(begin, end - begin);
    if (!item.empty()) {
      paths.emplace_back(item);
    }
    if (end == std::string::npos) {
      break;
    }
    begin = end + 1;
  }
  return paths;
}

void *openLibrary(const std::string &path) {
#ifdef _WIN32
  return reinterpret_cast<void *>(::LoadLibraryA(path.c_str()));
#else
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  ::dlerror();
  return ::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
#endif
}

void closeLibrary(void *handle) {
#ifdef _WIN32
  ::FreeLibrary(reinterpret_cast<HMODULE>(handle));
#else
  ::dlclose(handle);
#endif
}

void *findSymbol(void *handle, const char *name) {
#ifdef _WIN32
  return reinterpret_cast<void *>(
      ::GetProcAddress(reinterpret_cast<HMODULE>(handle), name));
#else
  return ::dlsym(handle, name);
#endif
}

std::string libraryError() {
#ifdef _WIN32
  return "Windows error " + std::to_string(::GetLastError());
#else
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  const char *reason = ::dlerror();
  return reason ? reason : "unknown error";
#endif
}

fs::path currentLibraryDirectory() {
#ifdef _WIN32
  HMODULE module = nullptr;
  if (!::GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                                GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                            reinterpret_cast<LPCSTR>(&currentLibraryDirectory),
                            &module)) {
    return {};
  }
  std::vector<char> buffer(MAX_PATH);
  for (;;) {
    const DWORD size = ::GetModuleFileNameA(module, buffer.data(),
                                            static_cast<DWORD>(buffer.size()));
    if (size == 0) {
      return {};
    }
    if (size < buffer.size() - 1) {
      return fs::path(std::string(buffer.data(), size)).parent_path();
    }
    buffer.resize(buffer.size() * 2);
  }
#else
  Dl_info info{};
  if (::dladdr(reinterpret_cast<const void *>(&currentLibraryDirectory),
               &info) &&
      info.dli_fname) {
    return fs::path(info.dli_fname).parent_path();
  }
  return {};
#endif
}

} // namespace

struct PluginLoader::Impl {
  std::vector<LoadedPlugin> plugins;
  // Successful plugins deliberately stay resident until process exit. Keeping
  // the raw handles without dlclose is intentional; the OS reclaims mappings.
  std::vector<void *> resident_handles;
};

PluginLoader::PluginLoader() : m_impl(std::make_unique<Impl>()) {}

PluginLoader::PluginLoader(PluginLoader &&) noexcept = default;
PluginLoader &PluginLoader::operator=(PluginLoader &&) noexcept = default;

PluginLoader::~PluginLoader() {
  if (!m_impl) {
    return;
  }
  // Registrations are scoped to this loader, but code remains mapped so node
  // instances and callbacks may safely outlive it.
  for (std::size_t i = m_impl->plugins.size(); i-- > 0;) {
    for (const auto &type : m_impl->plugins[i].registered_types) {
      NodeRegistry::instance().unregisterType(type);
    }
  }
}

namespace {

bool frameworkVersionCompatible(std::uint32_t plugin_version) {
  constexpr std::uint32_t k_host = AI_PIPE_VERSION;
  return (plugin_version / 100) == (k_host / 100);
}

std::string versionToString(std::uint32_t version) {
  return std::to_string(version / 10000) + "." +
         std::to_string((version / 100) % 100) + "." +
         std::to_string(version % 100);
}

std::string safeString(const char *value) { return value ? value : ""; }

} // namespace

Result<PluginLoader::LoadedPlugin> PluginLoader::load(const std::string &path) {
  const auto identity = canonicalIdentity(path);
  for (const auto &loaded : m_impl->plugins) {
    if (loaded.path == identity) {
      return Result<LoadedPlugin>::err(ErrorCode::InvalidArgument,
                                       "Plugin already loaded: " + identity);
    }
  }

  auto &registry = NodeRegistry::instance();
  const auto registry_snapshot = registry.snapshot();
  std::unordered_set<std::string> types_before;
  types_before.reserve(registry_snapshot.size());
  for (const auto &[type, factory] : registry_snapshot) {
    (void)factory;
    types_before.insert(type);
  }

  void *handle = openLibrary(identity);
  if (!handle) {
    return Result<LoadedPlugin>::err(ErrorCode::PluginLoadFailed,
                                     "Failed to open shared library '" +
                                         identity + "': " + libraryError());
  }

  auto reject = [&](ErrorCode code, const std::string &message) {
    registry.restore(registry_snapshot);
    closeLibrary(handle);
    return Result<LoadedPlugin>::err(code, message);
  };

  auto descriptor_entry = reinterpret_cast<PluginDescriptorFn>(
      findSymbol(handle, k_plugin_entry_symbol));
  if (!descriptor_entry) {
    return reject(ErrorCode::PluginSymbolMissing,
                  "'" + identity + "' is not an AI Pipe plugin (missing " +
                      std::string(k_plugin_entry_symbol) + ")");
  }

  const PluginDescriptor *descriptor = nullptr;
  try {
    descriptor = descriptor_entry();
  } catch (...) {
    return reject(ErrorCode::PluginLoadFailed,
                  "Plugin descriptor threw for '" + identity + "'");
  }
  if (!descriptor) {
    return reject(ErrorCode::PluginSymbolMissing,
                  "'" + identity + "' returned a null plugin descriptor");
  }
  if (descriptor->abi_version != k_plugin_abi_version) {
    return reject(ErrorCode::PluginVersionMismatch,
                  "'" + identity + "' uses plugin ABI revision " +
                      std::to_string(descriptor->abi_version) +
                      ", host expects " + std::to_string(k_plugin_abi_version));
  }
  if (!frameworkVersionCompatible(descriptor->ai_pipe_version)) {
    return reject(ErrorCode::PluginVersionMismatch,
                  "'" + identity + "' was built against ai_pipe " +
                      versionToString(descriptor->ai_pipe_version) +
                      ", host is " + versionToString(AI_PIPE_VERSION) +
                      " (major.minor must match)");
  }
  if (!descriptor->name || descriptor->name[0] == '\0') {
    return reject(ErrorCode::PluginLoadFailed,
                  "'" + identity + "' has an empty plugin name");
  }
  for (const auto &loaded : m_impl->plugins) {
    if (loaded.name == descriptor->name) {
      return reject(ErrorCode::InvalidArgument,
                    "Plugin name already loaded: " + loaded.name);
    }
  }
  if (descriptor->capability_count > 0 && !descriptor->capabilities) {
    return reject(ErrorCode::PluginLoadFailed,
                  "'" + identity + "' has an invalid capability array");
  }

  auto registration_entry = reinterpret_cast<PluginRegistrationFn>(
      findSymbol(handle, k_plugin_registration_symbol));
  if (!registration_entry) {
    return reject(ErrorCode::PluginSymbolMissing,
                  "'" + identity + "' is missing " +
                      std::string(k_plugin_registration_symbol));
  }

  bool registered = false;
  try {
    registered = registration_entry(registry);
  } catch (const std::exception &error) {
    return reject(ErrorCode::PluginLoadFailed,
                  "Plugin registration threw for '" + identity +
                      "': " + error.what());
  } catch (...) {
    return reject(ErrorCode::PluginLoadFailed,
                  "Plugin registration threw for '" + identity + "'");
  }

  std::vector<std::string> registered_types;
  for (const auto &type : registry.registeredTypes()) {
    if (!types_before.contains(type)) {
      registered_types.push_back(type);
    }
  }
  std::sort(registered_types.begin(), registered_types.end());
  if (!registered || registered_types.empty()) {
    return reject(
        ErrorCode::PluginLoadFailed,
        "Plugin registration failed or registered no node types for '" +
            identity + "'");
  }

  LoadedPlugin loaded;
  loaded.path = identity;
  loaded.name = descriptor->name;
  loaded.version = safeString(descriptor->version);
  loaded.provider = safeString(descriptor->provider);
  loaded.description = safeString(descriptor->description);
  loaded.ai_pipe_version = descriptor->ai_pipe_version;
  loaded.registered_types = std::move(registered_types);
  loaded.capabilities.reserve(descriptor->capability_count);
  for (std::uint32_t i = 0; i < descriptor->capability_count; ++i) {
    loaded.capabilities.emplace_back(safeString(descriptor->capabilities[i]));
  }

  LOG_INFO_S << "PluginLoader: Loaded plugin '" << loaded.name << "' from "
             << identity << " (" << loaded.registered_types.size()
             << " node types)";

  m_impl->resident_handles.push_back(handle);
  m_impl->plugins.push_back(loaded);
  return loaded;
}

Result<std::vector<PluginLoader::LoadedPlugin>>
PluginLoader::loadDirectory(const std::string &directory) {
  std::error_code error;
  if (!fs::is_directory(directory, error)) {
    return Result<std::vector<LoadedPlugin>>::err(
        ErrorCode::InvalidArgument, "Not a directory: " + directory);
  }

  std::vector<std::string> candidates;
  for (const auto &entry : fs::directory_iterator(directory, error)) {
    if (entry.is_regular_file() && isPluginCandidate(entry.path())) {
      candidates.push_back(entry.path().string());
    }
  }
  if (error) {
    return Result<std::vector<LoadedPlugin>>::err(
        ErrorCode::PluginLoadFailed,
        "Failed scanning '" + directory + "': " + error.message());
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
  const auto identity = canonicalIdentity(path);
  for (std::size_t i = 0; i < m_impl->plugins.size(); ++i) {
    if (m_impl->plugins[i].path != identity) {
      continue;
    }
    for (const auto &type : m_impl->plugins[i].registered_types) {
      NodeRegistry::instance().unregisterType(type);
    }
    m_impl->plugins.erase(m_impl->plugins.begin() +
                          static_cast<std::ptrdiff_t>(i));
    return Result<void>::ok();
  }
  return Result<void>::err(ErrorCode::InvalidArgument,
                           "Plugin not loaded: " + identity);
}

std::vector<std::filesystem::path> PluginLoader::defaultSearchPaths() const {
  // PluginLoader is startup-only and documented not thread-safe; callers must
  // not mutate the process environment concurrently.
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  auto paths = splitSearchPath(std::getenv("AI_PIPE_PLUGIN_PATH"));
  const auto library_directory = currentLibraryDirectory();
  if (!library_directory.empty()) {
    paths.push_back(library_directory / "ai_pipe" / "plugins");
  }
  return paths;
}

Result<std::vector<PluginLoader::LoadedPlugin>>
PluginLoader::discover(const std::vector<std::filesystem::path> &search_paths) {
  auto paths = search_paths;
  auto defaults = defaultSearchPaths();
  paths.insert(paths.end(), defaults.begin(), defaults.end());

  std::vector<LoadedPlugin> all_loaded;
  std::unordered_set<std::string> visited;
  for (const auto &path : paths) {
    std::error_code error;
    if (!fs::is_directory(path, error)) {
      continue;
    }
    const auto identity = canonicalIdentity(path.string());
    if (!visited.insert(identity).second) {
      continue;
    }
    auto loaded = loadDirectory(identity);
    if (!loaded) {
      return Result<std::vector<LoadedPlugin>>::err(loaded.error());
    }
    auto &plugins = loaded.value();
    all_loaded.insert(all_loaded.end(),
                      std::make_move_iterator(plugins.begin()),
                      std::make_move_iterator(plugins.end()));
  }
  return all_loaded;
}

const std::vector<PluginLoader::LoadedPlugin> &PluginLoader::plugins() const {
  return m_impl->plugins;
}

} // namespace ai_pipe
