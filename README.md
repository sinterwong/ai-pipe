# ai-pipe
<p align="center">
  <img src="assets/icon/ai-pipe-logo2.jpeg" alt="ai-pipe Logo" width="500"> <br/>
</p>

<p align="center">
  A lightweight pipeline framework based on DAG model.
</p>

The pipeline module framework supports DAG-based logic construction, enabling the direct creation of pipelines through configuration files once computing nodes are defined and registered. Plans include supporting additional platforms in the future.

## Features ✨
*   **Easy to use:** Simple API for inference.
*   **High decoupling and scalability:** Standardized interface design and new node registration process.

## Env 🛠️
*   CMake 3.15+
*   GCC 11+

## Build 🚀
```bash
git clone https://github.com/sinterwong/ai-pipe.git --recurse-submodules
cd ai-pipe
mkdir build
cd build
cmake ..
make -j$(nproc)
```

## Project Structure 🏗️

```
ai-pipe/
├── assets/
├── benchmarks/
├── cmake/
├── doc/
├── examples/
├── scripts/
├── src/
├── tests/
└── README.md
```

## Roadmap 🗺️

- [x] Design and implement native pipeline module
- [x] Build CI
- [ ] Support building a pipeline instance with another methods
