/**
 * @file test_graph_loader.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Tests for the JSON graph loader (F1, AI_PIPE_WITH_JSON)
 * @version 0.1
 * @date 2026-07-10
 *
 * The loader is an optional component: when the library is built
 * without AI_PIPE_WITH_JSON, only the stub-behavior test runs and the
 * rest are skipped, so this file compiles in every configuration.
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/graph_loader.hpp"
#include "ai_pipe/pipeline.hpp"
#include <cstdint>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace ai_pipe;

namespace ai_pipe_unit_test::graph_loader {

/// Source-shaped node: single declared output port
class JsonSourceNode : public ILogicNode {
public:
  explicit JsonSourceNode(const std::string &name) : ILogicNode(name) {}
  void process(const PortDataMap &, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    outputs["output"] = std::make_shared<PortData>();
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }
};

/// Pass-through-shaped node: one input, one output
class JsonPassNode : public ILogicNode {
public:
  explicit JsonPassNode(const std::string &name) : ILogicNode(name) {}
  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    for (const auto &[port, data] : inputs) {
      outputs["output"] = data;
    }
  }
  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }
};

/// Config-consuming node exposing what it received for assertions
class JsonConfiguredNode : public ILogicNode {
public:
  JsonConfiguredNode(const std::string &name, const PortData &config)
      : ILogicNode(name), m_config(config) {}
  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {}
  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  [[nodiscard]] const PortData &config() const { return m_config; }

private:
  PortData m_config;
};

AI_PIPE_REGISTER_NODE(JsonSourceNode);
AI_PIPE_REGISTER_NODE(JsonPassNode);
AI_PIPE_REGISTER_NODE_WITH_CONFIG(JsonConfiguredNode);

#define SKIP_WITHOUT_JSON()                                                    \
  if (!jsonGraphLoaderAvailable()) {                                           \
    GTEST_SKIP() << "Built without AI_PIPE_WITH_JSON";                         \
  }

TEST(GraphLoaderTest, StubReportsUnavailableWhenDisabled) {
  if (jsonGraphLoaderAvailable()) {
    GTEST_SKIP() << "Built with AI_PIPE_WITH_JSON";
  }
  auto result = loadGraphFromJson("{}");
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::InvalidConfiguration);
  EXPECT_NE(result.errorMessage().find("AI_PIPE_WITH_JSON"), std::string::npos);
}

TEST(GraphLoaderTest, LoadsNodesAndEdges) {
  SKIP_WITHOUT_JSON();
  const std::string doc = R"({
    "nodes": [
      {"type": "JsonSourceNode", "name": "src"},
      {"type": "JsonPassNode", "name": "pass"},
      {"type": "JsonConfiguredNode", "name": "sink"}
    ],
    "edges": [
      {"from": "src.output", "to": "pass.input"},
      {"from": {"node": "pass", "port": "output"},
       "to": {"node": "sink", "port": "input"}}
    ]
  })";
  auto result = loadGraphFromJson(doc);
  ASSERT_TRUE(result.isOk()) << result.errorMessage();

  const Graph &graph = result.value();
  EXPECT_EQ(graph.getNodes().size(), 3u);
  EXPECT_EQ(graph.getEdges().size(), 2u);
  ASSERT_NE(graph.getNode("src"), nullptr);
  ASSERT_NE(graph.getNode("pass"), nullptr);
  EXPECT_EQ(graph.getInDegree(graph.getNode("pass")), 1);
  EXPECT_EQ(graph.getOutDegree(graph.getNode("pass")), 1);
}

TEST(GraphLoaderTest, ConfigValuesArriveTyped) {
  SKIP_WITHOUT_JSON();
  const std::string doc = R"({
    "nodes": [
      {"type": "JsonConfiguredNode", "name": "cfg", "config": {
        "threshold": 0.75,
        "iterations": 3,
        "label": "person",
        "enabled": true,
        "scales": [0.5, 1, 2.0],
        "steps": [1, 2, 3],
        "classes": ["car", "bus"]
      }}
    ]
  })";
  auto result = loadGraphFromJson(doc);
  ASSERT_TRUE(result.isOk()) << result.errorMessage();

  auto node = std::dynamic_pointer_cast<JsonConfiguredNode>(
      result.value().getNode("cfg"));
  ASSERT_NE(node, nullptr);
  const PortData &config = node->config();
  EXPECT_DOUBLE_EQ(config.getParam<double>("threshold"), 0.75);
  EXPECT_EQ(config.getParam<std::int64_t>("iterations"), 3);
  EXPECT_EQ(config.getParam<std::string>("label"), "person");
  EXPECT_TRUE(config.getParam<bool>("enabled"));
  // Mixed int/float numeric array promotes to double
  EXPECT_EQ(config.getParam<std::vector<double>>("scales"),
            (std::vector<double>{0.5, 1.0, 2.0}));
  EXPECT_EQ(config.getParam<std::vector<std::int64_t>>("steps"),
            (std::vector<std::int64_t>{1, 2, 3}));
  EXPECT_EQ(config.getParam<std::vector<std::string>>("classes"),
            (std::vector<std::string>{"car", "bus"}));
}

TEST(GraphLoaderTest, PipelineOptionsUseModeDefaultsThenOverrides) {
  SKIP_WITHOUT_JSON();
  const std::string doc = R"({
    "nodes": [{"type": "JsonSourceNode", "name": "src"}],
    "options": {"mode": "stream", "num_workers": 2}
  })";
  auto result = loadPipelineFromJson(doc);
  ASSERT_TRUE(result.isOk()) << result.errorMessage();

  const PipelineOptions &options = result.value().options;
  EXPECT_EQ(options.mode, ExecutionMode::STREAM);
  EXPECT_EQ(options.num_workers, 2);
  // Untouched keys keep the stream() factory defaults
  EXPECT_EQ(options.queue_capacity, 16u);
  EXPECT_TRUE(options.enable_sync_coordination);
}

TEST(GraphLoaderTest, OmittedOptionsYieldDefaults) {
  SKIP_WITHOUT_JSON();
  const std::string doc =
      R"({"nodes": [{"type": "JsonSourceNode", "name": "src"}]})";
  auto result = loadPipelineFromJson(doc);
  ASSERT_TRUE(result.isOk()) << result.errorMessage();
  EXPECT_EQ(result.value().options.mode, ExecutionMode::BATCH);
  EXPECT_EQ(result.value().options.num_workers, 4);
}

TEST(GraphLoaderTest, LoadedDescriptionBuildsAPipeline) {
  SKIP_WITHOUT_JSON();
  const std::string doc = R"({
    "nodes": [
      {"type": "JsonSourceNode", "name": "src"},
      {"type": "JsonPassNode", "name": "pass"}
    ],
    "edges": [{"from": "src.output", "to": "pass.input"}],
    "options": {"mode": "batch", "num_workers": 2}
  })";
  auto description = loadPipelineFromJson(doc);
  ASSERT_TRUE(description.isOk()) << description.errorMessage();

  auto pipeline = Pipeline::create()
                      .withGraph(std::move(description.value().graph))
                      .withOptions(description.value().options)
                      .build();
  ASSERT_TRUE(pipeline.isOk()) << pipeline.errorMessage();
  EXPECT_TRUE(pipeline.value().isReady());
}

TEST(GraphLoaderTest, FileVariantLoads) {
  SKIP_WITHOUT_JSON();
  const std::string path =
      testing::TempDir() + "/ai_pipe_graph_loader_test.json";
  {
    std::ofstream out(path);
    out << R"({"nodes": [{"type": "JsonSourceNode", "name": "src"}]})";
  }
  auto result = loadGraphFromJsonFile(path);
  ASSERT_TRUE(result.isOk()) << result.errorMessage();
  EXPECT_EQ(result.value().getNodes().size(), 1u);

  auto missing = loadGraphFromJsonFile(path + ".does-not-exist");
  ASSERT_FALSE(missing.isOk());
  EXPECT_EQ(missing.errorCode(), ErrorCode::InvalidArgument);
}

TEST(GraphLoaderTest, MalformedJsonIsReported) {
  SKIP_WITHOUT_JSON();
  auto result = loadGraphFromJson("{ not json");
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::InvalidConfiguration);
  EXPECT_NE(result.errorMessage().find("parse error"), std::string::npos);
}

TEST(GraphLoaderTest, SchemaViolationsAreRejected) {
  SKIP_WITHOUT_JSON();
  const std::string source_only =
      R"("nodes": [{"type": "JsonSourceNode", "name": "src"}])";

  struct Case {
    const char *label;
    std::string doc;
    std::string expect_in_message;
  };
  const Case cases[] = {
      {"missing nodes", R"({"edges": []})", "'nodes'"},
      {"empty nodes", R"({"nodes": []})", "'nodes'"},
      {"unknown top-level key", "{" + source_only + R"(, "nodez": 1})",
       "nodez"},
      {"unknown node key",
       R"({"nodes": [{"type": "JsonSourceNode", "name": "s", "cfg": {}}]})",
       "cfg"},
      {"unknown node type",
       R"({"nodes": [{"type": "NoSuchNodeType", "name": "s"}]})",
       "NoSuchNodeType"},
      {"duplicate node name",
       R"({"nodes": [{"type": "JsonSourceNode", "name": "s"},
                     {"type": "JsonSourceNode", "name": "s"}]})",
       "Duplicate"},
      {"nested config object",
       R"({"nodes": [{"type": "JsonConfiguredNode", "name": "s",
                      "config": {"nested": {"a": 1}}}]})",
       "nested"},
      {"endpoint without dot",
       "{" + source_only + R"(, "edges": [{"from": "src", "to": "src.x"}]})",
       "node.port"},
      {"undeclared port",
       R"({"nodes": [{"type": "JsonSourceNode", "name": "a"},
                     {"type": "JsonPassNode", "name": "b"}],
           "edges": [{"from": "a.bogus", "to": "b.input"}]})",
       "rejected"},
      {"unknown option key",
       "{" + source_only + R"(, "options": {"workers": 4}})", "workers"},
      {"bad mode", "{" + source_only + R"(, "options": {"mode": "turbo"}})",
       "turbo"},
      {"zero workers",
       "{" + source_only + R"(, "options": {"num_workers": 0}})",
       "num_workers"},
  };

  for (const Case &c : cases) {
    auto result = loadGraphFromJson(c.doc);
    auto pipeline_result = loadPipelineFromJson(c.doc);
    // Option errors only surface through the pipeline variant
    ASSERT_FALSE(pipeline_result.isOk()) << c.label;
    EXPECT_NE(pipeline_result.errorMessage().find(c.expect_in_message),
              std::string::npos)
        << c.label << ": " << pipeline_result.errorMessage();
    (void)result;
  }
}

TEST(GraphLoaderTest, CycleIsRejected) {
  SKIP_WITHOUT_JSON();
  const std::string doc = R"({
    "nodes": [
      {"type": "JsonPassNode", "name": "a"},
      {"type": "JsonPassNode", "name": "b"}
    ],
    "edges": [
      {"from": "a.output", "to": "b.input"},
      {"from": "b.output", "to": "a.input"}
    ]
  })";
  auto result = loadGraphFromJson(doc);
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::GraphCycleDetected);
}

} // namespace ai_pipe_unit_test::graph_loader
