# Node plugin guide

Dynamic plugins require a shared AI Pipe build. A plugin exports a POD
descriptor and a versioned explicit registration entry:

```cpp
#include <ai_pipe/node_registry.hpp>
#include <ai_pipe/plugin.hpp>

AI_PIPE_PLUGIN_WITH_METADATA("acme.video", "1.2.0", "acme",
                             "Video processing nodes");

extern "C" AI_PIPE_PLUGIN_EXPORT bool
ai_pipe_register_plugin_v1(ai_pipe::NodeRegistry &registry) {
  auto decoder = registry.registerConfiguredNode<DecoderNode>(
      "acme.video.decoder");
  if (!decoder) {
    return false;
  }
  return registry.registerNode<DisplayNode>("acme.video.display").isOk();
}
```

Type ids are configuration protocol, not C++ implementation names. Use a
stable reverse-domain or vendor/category/name convention and never silently
reuse an id for incompatible behavior.

External CMake projects use the installed helper:

```cmake
find_package(ai_pipe REQUIRED CONFIG)
ai_pipe_add_plugin(acme_video
    SOURCES decoder.cpp display.cpp register.cpp
    DEPENDENCIES vendor::codec)
```

The default output name is `ai_pipe_plugin_<target>`. Plugins install to
`lib/ai_pipe/plugins` on Unix-like systems and `bin/ai_pipe/plugins` on Windows.
`PluginLoader::discover()` searches explicit paths, `AI_PIPE_PLUGIN_PATH`, and
the installation-relative plugin directory.

Registration happens only after the descriptor passes its version checks. It is
transactional and must return false when any factory registration fails. Do not
use `AI_PIPE_REGISTER_NODE` in a dynamic plugin; that macro is retained for
nodes compiled directly into a monolithic application.

Successful plugins stay mapped until process exit. `unload()` deactivates their
factories but deliberately does not unmap their code.
