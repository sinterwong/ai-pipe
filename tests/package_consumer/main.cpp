#include <ai_pipe/graph_loader.hpp>
#include <ai_pipe/node_registry.hpp>
#include <ai_pipe/plugin.hpp>

#include <iostream>

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "usage: ai_pipe_package_consumer <plugin>\n";
    return 2;
  }

  ai_pipe::PluginLoader loader;
  auto plugin = loader.load(argv[1]);
  if (!plugin) {
    std::cerr << plugin.error().toString() << '\n';
    return 3;
  }

  auto node = ai_pipe::NodeRegistry::instance().create("package.echo", "echo");
  if (!node) {
    std::cerr << node.error().toString() << '\n';
    return 4;
  }

  auto graph = ai_pipe::loadGraphFromJson(
      R"({"nodes":[{"type":"package.echo","name":"echo"}],"edges":[]})");
  if (!graph || graph.value().getNodes().size() != 1) {
    std::cerr << (graph ? "unexpected graph" : graph.error().toString())
              << '\n';
    return 5;
  }
  return 0;
}
