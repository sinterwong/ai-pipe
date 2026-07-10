/**
 * @file test_plugin_loader.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Dynamic plugin loading tests (F8)
 *
 * Uses the plugins built under tests/plugins/: a valid one (declares
 * AI_PIPE_PLUGIN and registers PluginEchoNode), plus two rejects in
 * bad/ - a foreign library without the descriptor symbol and one with
 * an incompatible plugin ABI revision. Rejected plugins must leave no
 * trace in the NodeRegistry.
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/node_registry.hpp"
#include "ai_pipe/plugin.hpp"
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>

using namespace ai_pipe;

namespace ai_pipe_unit_test::plugin_loader {

/// Directory holding the valid test plugin (bad ones live in bad/)
std::string pluginRoot() {
  if (const char *env = std::getenv("AI_PIPE_TEST_PLUGIN_DIR")) {
    return env;
  }
  // Running from the install tree (tests are executed with cwd=install)
  if (std::filesystem::is_directory("tests/plugins")) {
    return "tests/plugins";
  }
  return AI_PIPE_TEST_PLUGIN_DIR;
}

std::string validPluginPath() {
  return pluginRoot() + "/libai_pipe_test_plugin.so";
}

class PluginLoaderTest : public ::testing::Test {
protected:
  void SetUp() override {
    ASSERT_TRUE(std::filesystem::exists(validPluginPath()))
        << "test plugin not built at " << validPluginPath();
    ASSERT_FALSE(NodeRegistry::instance().isRegistered("PluginEchoNode"))
        << "registry polluted by a previous test";
  }
};

TEST_F(PluginLoaderTest, LoadRegistersAndUnloadRemovesNodeTypes) {
  PluginLoader loader;

  auto result = loader.load(validPluginPath());
  ASSERT_TRUE(result.isOk()) << result.error().toString();

  const auto &plugin = result.value();
  EXPECT_EQ(plugin.name, "test_plugin");
  EXPECT_EQ(plugin.registered_types,
            (std::vector<std::string>{"PluginEchoNode"}));
  EXPECT_TRUE(NodeRegistry::instance().isRegistered("PluginEchoNode"));
  ASSERT_EQ(loader.plugins().size(), 1u);

  // The registered factory must produce working node instances. The
  // instance is scoped: nothing from the plugin may outlive unload().
  {
    auto node = NodeRegistry::instance().create("PluginEchoNode", "echo1");
    ASSERT_TRUE(node.isOk());
    EXPECT_EQ(node.value()->getName(), "echo1");
    EXPECT_EQ(node.value()->getExpectedInputPorts(),
              (std::vector<std::string>{"input"}));
  }

  ASSERT_TRUE(loader.unload(validPluginPath()).isOk());
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("PluginEchoNode"));
  EXPECT_TRUE(loader.plugins().empty());

  // Re-loading after a real unload must re-run static registration
  // (plugins are built with -fno-gnu-unique so dlclose truly unmaps).
  auto reloaded = loader.load(validPluginPath());
  ASSERT_TRUE(reloaded.isOk()) << reloaded.error().toString();
  EXPECT_EQ(reloaded.value().registered_types,
            (std::vector<std::string>{"PluginEchoNode"}));
  ASSERT_TRUE(loader.unload(validPluginPath()).isOk());
}

TEST_F(PluginLoaderTest, DestructorUnregistersPluginTypes) {
  {
    PluginLoader loader;
    ASSERT_TRUE(loader.load(validPluginPath()).isOk());
    ASSERT_TRUE(NodeRegistry::instance().isRegistered("PluginEchoNode"));
  }
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("PluginEchoNode"));
}

TEST_F(PluginLoaderTest, RejectsForeignLibraryAndRollsBack) {
  PluginLoader loader;

  const std::string path =
      pluginRoot() + "/bad/libai_pipe_test_plugin_noentry.so";
  ASSERT_TRUE(std::filesystem::exists(path));

  auto result = loader.load(path);
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.error().code(), ErrorCode::PluginSymbolMissing);

  // Whatever it registered during dlopen static init must be gone.
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("OrphanPluginNode"));
  EXPECT_TRUE(loader.plugins().empty());
}

TEST_F(PluginLoaderTest, RejectsAbiMismatchAndRollsBack) {
  PluginLoader loader;

  const std::string path =
      pluginRoot() + "/bad/libai_pipe_test_plugin_badversion.so";
  ASSERT_TRUE(std::filesystem::exists(path));

  auto result = loader.load(path);
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.error().code(), ErrorCode::PluginVersionMismatch);
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("BadVersionNode"));
}

TEST_F(PluginLoaderTest, LoadDirectoryScansAndRejectsDuplicates) {
  PluginLoader loader;

  auto result = loader.loadDirectory(pluginRoot());
  ASSERT_TRUE(result.isOk()) << result.error().toString();
  ASSERT_EQ(result.value().size(), 1u)
      << "top-level plugin dir must contain exactly the valid plugin";
  EXPECT_EQ(result.value()[0].name, "test_plugin");

  // Same path again through load(): must be refused, not double-loaded.
  auto duplicate = loader.load(validPluginPath());
  ASSERT_FALSE(duplicate.isOk());
  EXPECT_EQ(duplicate.error().code(), ErrorCode::InvalidArgument);

  auto missing = loader.loadDirectory(pluginRoot() + "/does_not_exist");
  EXPECT_FALSE(missing.isOk());
}

} // namespace ai_pipe_unit_test::plugin_loader
