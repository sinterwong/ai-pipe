# 16. 动态节点注册与插件系统 (PluginLoader)

在现代大型 AI 管道或工业级流媒体系统中，整个应用程序的体积通常异常庞大。如果将所有的算法节点（如不同的算法检测模型、后处理渲染、定制推流模块）全部静态链接（Static Link）到主程序中，会导致主程序极其臃肿，且只要任何一个算法节点做了微调，整个系统都必须重新构建和部署。

AI Pipe 手搓了一套支持 **运行时热加载（Runtime Dynamic Loading）**、**注册表快照安全回滚（Registry Snapshot Rollback）**以及 **ABI 握手校验** 的 C-Linkage 插件系统。

---

## 1. 核心设计原理

### 1.1 ABI 边界、C-Linkage 与版本握手（Version Handshake）
C++ 的类符号在编译后会经历严重的 **名字粉碎（Name Mangling）**。不同版本的编译器、甚至不同编译参数下编译出的 C++ 动态库，其名字粉碎规则和虚函数表（vtable）内存布局存在巨大差异。

如果主程序直接通过 C++ 接口去加载动态库中的类，会产生极其致命的 **二进制不兼容崩溃（Undefined Behavior / Segfault）**。

**解决方案**：
1.  **极简 C-Linkage 导出**：
    跨越 `dlopen` 物理边界的只提供一个遵循 C 标准规范（无 Mangling、布局极度固化）的不透明 C 符号：
    ```cpp
    extern "C" {
        AI_PIPE_EXPORT const PluginDescriptor* ai_pipe_plugin_descriptor();
    }
    ```
2.  **PluginDescriptor 版本握手**：
    `PluginDescriptor` 必须是一个 **Standard-Layout C-Style 结构体**（成员只有基础类型、不包含 `std::string` 等 C++ 复杂容器，彻底规避不同标准库实现差异）。
    *   主程序通过 `dlsym` 读取该描述符：
        *   强校验插件协议修订号 `k_plugin_abi_version`。
        *   在框架预发布（Pre-1.0）阶段，强校验 `major.minor` 版本号必须完全一致，否则拒绝加载。

---

## 2. 核心巧思与实现细节

### 2.1 注册表快照备份与失败安全回滚（Fail-Safe Rollback）
动态插件中最危险的一点在于：插件在静态初始化（Static Initialization）或加载阶段，会高频往全局单例 `NodeRegistry` 注册新的节点类（`AI_PIPE_REGISTER_NODE`）。

如果插件加载到中途，突然发生**版本握手失败（Version Mismatch）**或**缺失符号错误**，我们必须对插件调用 `dlclose` 进行物理卸载。
*   **悬挂指针危机（Dangling Pointer）**：由于插件物理库被卸载，其贡献的代码段（Code Segment）和注册回调函数指针已在内存中不复存在。如果全局注册表 `NodeRegistry` 中依然残留着这些节点的名字和回调函数，当其他管线尝试实例化该节点时，调用这些悬挂指针将引发主程序瞬间崩溃（Segfault）。

**实现巧思（Rollback Snapshot Technique）**：
在 `PluginLoader` 启动加载（`dlopen`）的第一瞬间：
1.  **保存快照**：
    对全局 `NodeRegistry` 保存一份当前的**键值对快照备份（Snapshot）**：
    `auto snapshot = NodeRegistry::instance().getRegisteredTypes();`
2.  **尝试 dlopen**：
    拉起 `dlopen`，触发动态库中所有 `static` 注册器的运行，向 `NodeRegistry` 塞入新节点。
3.  **握手与校验**：
    进行 `ai_pipe_plugin_descriptor` 握手。
    *   若**握手失败**：
        立即进行**安全回滚**：
        ```cpp
        NodeRegistry::instance().restoreSnapshot(snapshot);
        ```
        随后再调用 `dlclose(handle)` 彻底物理注销库。
    *   **效果**：这保证了加载失败时，注册表被完美重置到加载前的绝对安全状态，杜绝了任何引发野指针和悬挂回调的致命隐患。

### 2.2 卸载边界与 `-fno-gnu-unique` 编译避坑
在 Linux GNU 环境下，如果类包含模板或 `static` 局部变量，编译器默认会为符号打上 `STB_GNU_UNIQUE` 标记。这会导致 glibc 即使看到了 `dlclose`，也会在内核中强行将该动态库标记为 `NODELETE`，物理内存拒绝卸载，重载该插件无法重新触发静态初始化。

**实现巧思**：
主程序以及动态插件必须显式添加编译选项：
`-fno-gnu-unique`
这能强制 glibc 遵守 `dlclose` 的物理卸载指令，使得热插拔和安全回滚能真正释放物理内存。

---

## 3. 手搓实现参考骨架

你可以根据以下完美防野指针、防崩溃的 `PluginLoader` 骨架进行手搓复习：

```cpp
struct PluginDescriptor {
    uint32_t abiVersion;
    uint32_t majorVersion;
    uint32_t minorVersion;
    const char* pluginName;
};

// 注册表单例（简化版）
class NodeRegistry {
public:
    static NodeRegistry& instance() {
        static NodeRegistry reg;
        return reg;
    }

    std::unordered_set<std::string> getRegisteredTypes() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_types;
    }

    void restoreSnapshot(const std::unordered_set<std::string>& snapshot) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_types = snapshot; // 强行回滚
    }

    void registerType(const std::string& name) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_types.insert(name);
    }

private:
    std::mutex m_mutex;
    std::unordered_set<std::string> m_types;
};

// ==================== 插件加载器 ====================
class PluginLoader {
public:
    PluginLoader() = default;
    ~PluginLoader() { unload(); }

    Result<void> load(const std::string& filepath) {
        // 1. 抓取快照备份
        auto snapshot = NodeRegistry::instance().getRegisteredTypes();

        // 2. 加载物理库
        void* handle = dlopen(filepath.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (!handle) {
            return Error(ErrorCode::InvalidArgument, "dlopen failed: " + std::string(dlerror()));
        }

        // 3. 寻找握手描述符
        auto descriptorFunc = reinterpret_cast<const PluginDescriptor*(*)()>(dlsym(handle, "ai_pipe_plugin_descriptor"));
        if (!descriptorFunc) {
            // 回滚并物理卸载
            NodeRegistry::instance().restoreSnapshot(snapshot);
            dlclose(handle);
            return Error(ErrorCode::InvalidArgument, "Missing plugin descriptor symbol");
        }

        const auto* desc = descriptorFunc();

        // 4. ABI 握手校验
        if (desc->abiVersion != 5 || desc->majorVersion != 0) {
            // 握手失败！无缝回滚
            NodeRegistry::instance().restoreSnapshot(snapshot);
            dlclose(handle);
            return Error(ErrorCode::InvalidArgument, "Plugin ABI/Version mismatch");
        }

        // 加载成功，登记句柄
        m_handles.push_back(handle);
        return {};
    }

    void unload() {
        for (void* handle : m_handles) {
            if (handle) dlclose(handle);
        }
        m_handles.clear();
    }

private:
    std::vector<void*> m_handles;
};
```

手搓 `PluginLoader` 期间，你将深刻理解“物理生命周期跨越动态边界（Snapshot Rollback）”的安全美学。这是 C++ 热拔插系统架构设计中，唯一能保证主程序 7×24 小时高可靠运行、决不因插件错误引发 Segment Fault 灾难的核心保障。
