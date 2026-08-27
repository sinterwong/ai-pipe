# External node plugin

This directory is a standalone consumer of an installed AI Pipe package. It is
intentionally not added by the repository's top-level CMake project: the example
uses only the public headers and exported `ai_pipe::ai_pipe` target available to
third-party plugins.

Build and install AI Pipe as a shared library, then configure this directory
against that installation:

```bash
cmake -S . -B build -DBUILD_SHARED_LIBS=ON \
  -DCMAKE_INSTALL_PREFIX="$PWD/install"
cmake --build build --target install

cmake -S examples/external_plugin -B build/external_plugin \
  -DCMAKE_PREFIX_PATH="$PWD/install"
cmake --build build/external_plugin
```

The resulting `ai_pipe_plugin_example_echo` shared library exports the AI Pipe
plugin descriptor and explicitly registers the stable node id `example.echo`.
Load it with
`ai_pipe::PluginLoader` before constructing a node through
`ai_pipe::NodeRegistry`.

The host and plugin must use the same shared AI Pipe library. Linking a static
copy into the plugin would create a separate process-wide registry and hide its
node registration from the host. Successful plugin libraries stay mapped until
process exit; `PluginLoader::unload()` deactivates their factories without
unmapping code that existing nodes or callbacks may still use.
