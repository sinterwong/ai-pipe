# AI Pipe ABI and versioning policy

This policy applies from the 0.5 plugin ABI v2 transition and is the contract to
freeze for 1.0.

## Public compatibility surface

The installed headers under `include/ai_pipe`, the CMake targets
`ai_pipe::ai_pipe` and optional `ai_pipe::config`, the plugin descriptor, and
the `ai_pipe_register_plugin_v1` entrypoint form the supported public surface.
Files under `src/`, `config/`, `tests/`, and `benchmarks/` are implementation
details unless installed explicitly.

Semantic-version rules:

- Patch releases preserve source and binary compatibility.
- Minor releases before 1.0 may change C++ ABI, but plugin loading requires the
  same major and minor version and reports `PluginVersionMismatch` otherwise.
- Starting with 1.0, minor releases preserve the documented public C++ ABI;
  removals and incompatible layout or virtual-interface changes require a major
  release.
- APIs are deprecated for at least one minor release before removal after 1.0.

## Plugin boundary

Only the descriptor lookup is a toolchain-neutral C ABI. After the descriptor
handshake, plugins exchange `NodeRegistry`, factories, `shared_ptr`, strings,
`std::any`, and `ILogicNode` virtual calls with the host. Consequently a plugin
must use:

- the same AI Pipe major/minor release;
- an ABI-compatible compiler and C++ standard library;
- compatible runtime, exception, RTTI, iterator-debugging, and architecture
  settings;
- the same shared `ai_pipe` library as the host.

The handshake cannot detect toolchain ABI drift. Products distributing binary
plugins must publish and enforce a supported toolchain tuple. A stable opaque C
node ABI is explicitly outside the 1.0 scope and should be added only if
third-party plugins must cross toolchain boundaries.

`k_plugin_abi_version` changes whenever the descriptor or registration protocol
changes. The versioned registration symbol changes only for an incompatible
registration contract.

## Plugin lifetime

Registration is transactional. A missing entrypoint, incompatible descriptor,
exception, `false` return, or empty registration restores the complete registry
snapshot and closes the rejected library.

Successfully loaded libraries stay mapped until process exit. `unload()` only
deactivates factories. This guarantees that existing nodes, callbacks, static
objects, and cross-plugin dependencies never point into unmapped code.
