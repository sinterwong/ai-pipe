/**
 * @file test_node_registry.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Tests for NodeRegistry and registration macros (P6.1)
 * @version 0.1
 * @date 2026-07-04
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/graph.hpp"
#include "ai_pipe/node_registry.hpp"
#include "helper_nodes.hpp"
#include <gtest/gtest.h>

using namespace ai_pipe;

namespace ai_pipe_unit_test::node_registry {

/// Simple registrable node taking only a name
class MacroNode : public ILogicNode {
public:
  explicit MacroNode(const std::string &name) : ILogicNode(name) {}
  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {}
};

/// Node configured through the PortData config bag
class ConfiguredNode : public ILogicNode {
public:
  ConfiguredNode(const std::string &name, const PortData &config)
      : ILogicNode(name),
        m_threshold(config.getOptionalParam<double>("threshold").value_or(0.5)) {
  }
  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {}
  [[nodiscard]] double threshold() const { return m_threshold; }

private:
  double m_threshold;
};

AI_PIPE_REGISTER_NODE(MacroNode);
AI_PIPE_REGISTER_NODE_WITH_CONFIG(ConfiguredNode);

TEST(NodeRegistryTest, MacroRegistrationAndCreate) {
  auto &registry = NodeRegistry::instance();
  ASSERT_TRUE(registry.isRegistered("MacroNode"));

  auto result = registry.create("MacroNode", "instance_a");
  ASSERT_TRUE(result.isOk());
  ASSERT_NE(result.value(), nullptr);
  EXPECT_EQ(result.value()->getName(), "instance_a");
}

TEST(NodeRegistryTest, ConfigDrivenCreate) {
  auto &registry = NodeRegistry::instance();
  ASSERT_TRUE(registry.isRegistered("ConfiguredNode"));

  PortData config;
  config.setParam("threshold", 0.9);

  auto result = registry.create("ConfiguredNode", "detector", config);
  ASSERT_TRUE(result.isOk());
  auto node = std::dynamic_pointer_cast<ConfiguredNode>(result.value());
  ASSERT_NE(node, nullptr);
  EXPECT_DOUBLE_EQ(node->threshold(), 0.9);

  // Default when config omits the param
  auto defaulted = registry.create("ConfiguredNode", "detector2");
  ASSERT_TRUE(defaulted.isOk());
  EXPECT_DOUBLE_EQ(
      std::dynamic_pointer_cast<ConfiguredNode>(defaulted.value())->threshold(),
      0.5);
}

TEST(NodeRegistryTest, UnknownTypeFails) {
  auto result = NodeRegistry::instance().create("NoSuchType", "x");
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::InvalidArgument);
  EXPECT_NE(result.errorMessage().find("NoSuchType"), std::string::npos);
}

TEST(NodeRegistryTest, DuplicateRegistrationRejected) {
  auto &registry = NodeRegistry::instance();
  const std::string type = "DupTestType";
  registry.unregisterType(type);

  auto factory = [](const std::string &name, const PortData &)
      -> Result<std::shared_ptr<ILogicNode>> {
    return std::shared_ptr<ILogicNode>(std::make_shared<MacroNode>(name));
  };

  EXPECT_TRUE(registry.registerFactory(type, factory).isOk());
  auto second = registry.registerFactory(type, factory);
  EXPECT_FALSE(second.isOk());
  EXPECT_EQ(second.errorCode(), ErrorCode::InvalidArgument);

  registry.unregisterType(type);
  EXPECT_FALSE(registry.isRegistered(type));
}

TEST(NodeRegistryTest, RegisteredNodesWorkInGraph) {
  auto &registry = NodeRegistry::instance();

  // Register pipeline-capable helper nodes under custom type names
  registry.unregisterType("TestPassThrough");
  ASSERT_TRUE(registry
                  .registerFactory(
                      "TestPassThrough",
                      [](const std::string &name, const PortData &)
                          -> Result<std::shared_ptr<ILogicNode>> {
                        return std::shared_ptr<ILogicNode>(
                            std::make_shared<PassThroughNode>(name));
                      })
                  .isOk());

  Graph graph;
  auto a = registry.create("TestPassThrough", "a");
  auto b = registry.create("TestPassThrough", "b");
  ASSERT_TRUE(a.isOk());
  ASSERT_TRUE(b.isOk());
  ASSERT_TRUE(graph.addNode(a.value()));
  ASSERT_TRUE(graph.addNode(b.value()));
  EXPECT_TRUE(graph.addEdge("a", "output", "b", "input"));

  registry.unregisterType("TestPassThrough");
}

} // namespace ai_pipe_unit_test::node_registry
