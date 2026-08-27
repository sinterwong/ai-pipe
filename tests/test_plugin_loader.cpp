#include "ai_pipe/node_registry.hpp"
#include "ai_pipe/plugin.hpp"
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>

using namespace ai_pipe;

namespace ai_pipe_unit_test::plugin_loader {

// Directory holding the valid test plugin (bad ones live in bad/)
std::string pluginRoot() {
  if (const char *env = std::getenv("AI_PIPE_TEST_PLUGIN_DIR")) {
    return env;
  }
  // Running from the install tree (tests are executed with cwd=install)
  if (std::filesystem::is_regular_file(std::filesystem::path("tests/plugins") /
                                       AI_PIPE_TEST_PLUGIN_FILENAME)) {
    return "tests/plugins";
  }
  return AI_PIPE_TEST_PLUGIN_DIR;
}

std::string validPluginPath() {
  return pluginRoot() + "/" + AI_PIPE_TEST_PLUGIN_FILENAME;
}

class PluginLoaderTest : public ::testing::Test {
protected:
  void SetUp() override {
    ASSERT_TRUE(std::filesystem::exists(validPluginPath()))
        << "test plugin not built at " << validPluginPath();
    ASSERT_FALSE(NodeRegistry::instance().isRegistered("test.echo"))
        << "registry polluted by a previous test";
  }
};

TEST_F(PluginLoaderTest, LoadRegistersAndDeactivateRemovesNodeTypes) {
  PluginLoader loader;

  auto result = loader.load(validPluginPath());
  ASSERT_TRUE(result.isOk()) << result.error().toString();

  const auto &plugin = result.value();
  EXPECT_EQ(plugin.name, "test_plugin");
  EXPECT_EQ(plugin.version, AI_PIPE_VERSION_STR);
  EXPECT_EQ(plugin.capabilities, (std::vector<std::string>{"node"}));
  EXPECT_EQ(plugin.registered_types, (std::vector<std::string>{"test.echo"}));
  EXPECT_TRUE(NodeRegistry::instance().isRegistered("test.echo"));
  ASSERT_EQ(loader.plugins().size(), 1u);

  // The registered factory must produce working node instances.
  {
    auto node = NodeRegistry::instance().create("test.echo", "echo1");
    ASSERT_TRUE(node.isOk());
    EXPECT_EQ(node.value()->getName(), "echo1");
    EXPECT_EQ(node.value()->getExpectedInputPorts(),
              (std::vector<std::string>{"input"}));
  }

  ASSERT_TRUE(loader.unload(validPluginPath()).isOk());
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("test.echo"));
  EXPECT_TRUE(loader.plugins().empty());

  // Re-activation invokes the explicit registration entry again. The code
  // mapping remains resident throughout.
  auto reloaded = loader.load(validPluginPath());
  ASSERT_TRUE(reloaded.isOk()) << reloaded.error().toString();
  EXPECT_EQ(reloaded.value().registered_types,
            (std::vector<std::string>{"test.echo"}));
  ASSERT_TRUE(loader.unload(validPluginPath()).isOk());
}

TEST_F(PluginLoaderTest, DestructorUnregistersPluginTypes) {
  {
    PluginLoader loader;
    ASSERT_TRUE(loader.load(validPluginPath()).isOk());
    ASSERT_TRUE(NodeRegistry::instance().isRegistered("test.echo"));
  }
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("test.echo"));
}

TEST_F(PluginLoaderTest, DeactivateKeepsExistingInstancesSafe) {
  PluginLoader loader;
  ASSERT_TRUE(loader.load(validPluginPath()).isOk());

  auto node = NodeRegistry::instance().create("test.echo", "echo_live");
  ASSERT_TRUE(node.isOk());

  ASSERT_TRUE(loader.unload(validPluginPath()).isOk());
  EXPECT_TRUE(loader.plugins().empty());
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("test.echo"));
  // Deactivation unregisters factories but keeps code mapped until process
  // exit, so already-created instances stay valid.
  EXPECT_EQ(node.value()->getExpectedInputPorts(),
            (std::vector<std::string>{"input"}));
  node.value().reset();
}

TEST_F(PluginLoaderTest, RejectsForeignLibraryAndRollsBack) {
  PluginLoader loader;

  const std::string path =
      pluginRoot() + "/bad/" + AI_PIPE_TEST_PLUGIN_NOENTRY_FILENAME;
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
      pluginRoot() + "/bad/" + AI_PIPE_TEST_PLUGIN_BADVERSION_FILENAME;
  ASSERT_TRUE(std::filesystem::exists(path));

  auto result = loader.load(path);
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.error().code(), ErrorCode::PluginVersionMismatch);
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("BadVersionNode"));
}

TEST_F(PluginLoaderTest, RegistrationFailureRollsBackTransaction) {
  PluginLoader loader;

  const std::string path =
      pluginRoot() + "/bad/" + AI_PIPE_TEST_PLUGIN_REJECT_FILENAME;
  ASSERT_TRUE(std::filesystem::exists(path));

  auto result = loader.load(path);
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.error().code(), ErrorCode::PluginLoadFailed);
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("test.rejected"));
  EXPECT_TRUE(loader.plugins().empty());
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

TEST_F(PluginLoaderTest, DiscoverLoadsExplicitSearchPaths) {
  PluginLoader loader;
  auto result = loader.discover({pluginRoot()});
  ASSERT_TRUE(result.isOk()) << result.error().toString();
  ASSERT_EQ(result.value().size(), 1u);
  EXPECT_EQ(result.value().front().registered_types,
            (std::vector<std::string>{"test.echo"}));
}

TEST_F(PluginLoaderTest, DestructorKeepsLibraryMappedWhileInstancesAlive) {
  std::shared_ptr<ILogicNode> survivor;
  {
    PluginLoader loader;
    ASSERT_TRUE(loader.load(validPluginPath()).isOk());
    auto node = NodeRegistry::instance().create("test.echo", "echo_survivor");
    ASSERT_TRUE(node.isOk());
    survivor = node.value();
  }
  // The loader is gone and the type unregistered, but the library must
  // still be mapped: virtual calls and (below) the destructor run
  // plugin code. Before instance tracking this was use-after-unmap.
  EXPECT_FALSE(NodeRegistry::instance().isRegistered("test.echo"));
  EXPECT_EQ(survivor->getName(), "echo_survivor");
  EXPECT_EQ(survivor->getExpectedOutputPorts(),
            (std::vector<std::string>{"output"}));
  survivor.reset();
}

} // namespace ai_pipe_unit_test::plugin_loader
