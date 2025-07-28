# ai-pipe
The pipeline module framework supports DAG-based logic construction, enabling the direct creation of pipelines through configuration files once computing nodes are defined and registered. Plans include supporting additional platforms in the future.

## Features ✨
*   **Cross-platform:** Supports Android and Linux.
*   **Easy to use:** Simple API for inference.
*   **High decoupling and scalability:** Standardized interface design and new node registration process.

## Env 🛠️
*   CMake 3.15+
*   GCC 11+
*   IDE VSCode
*   Ubuntu 24.04

## Build 🚀

### Manual Dependency Management

1. **Link Libraries:** Symbolically link manually compiled libraries to `/repo/3rdparty/target/${TARGET_OS}_${TARGET_ARCH}` (e.g., `/repo/3rdparty/target/Linux_x86_64/opencv`).
2. **Manage Dependencies:** Use `/repo/load_3rdparty.cmake` to manage the loading of your libraries.
3. You can download dependencies from the following links:
    *   [Android_aarch64](https://github.com/sinterwong/ai-pipe/releases/download/v0.2.0-alpha/dependency_Android_aarch64.tgz)
    *   [Linux_x86_64](https://github.com/sinterwong/ai-pipe/releases/download/v0.2.0-alpha/dependency_Linux_x86_64.tgz)
    *   decompress it to `/repo/3rdparty/target/${TARGET_OS}_${TARGET_ARCH}`

## Project Structure 🏗️

```
ai-pipe/
├── src/
├── cmake/
├── scripts/
├── platform/
├── tests/
└── README.md
```

## Roadmap 🗺️

- [x] Design and implement native pipeline module
- [x] Build CI
- [x] Implement a demo module that combines a complete algorithm module and a pipeline module.
