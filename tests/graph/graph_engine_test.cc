// Copyright 2025-present the zvec project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "graph/graph_engine.h"

using namespace zvec;
using namespace zvec::graph;

class GraphEngineTest : public testing::Test {
 protected:
  static std::string test_dir_;

  GraphSchema MakeTestSchema() {
    GraphSchema schema("test_graph");

    NodeTypeBuilder nb("table");
    nb.AddProperty("database", zvec::proto::DT_STRING, false);
    schema.AddNodeType(nb.Build());

    NodeTypeBuilder nb2("column");
    nb2.AddProperty("data_type", zvec::proto::DT_STRING, false);
    schema.AddNodeType(nb2.Build());

    EdgeTypeBuilder eb("has_column", true);
    schema.AddEdgeType(eb.Build());
    schema.AddEdgeConstraint("has_column", "table", "column");

    return schema;
  }

  GraphNode MakeTableNode(const std::string& id,
                          const std::string& database) {
    GraphNode node;
    node.id = id;
    node.node_type = "table";
    node.properties["database"] = database;
    return node;
  }

  GraphNode MakeColumnNode(const std::string& id,
                           const std::string& data_type) {
    GraphNode node;
    node.id = id;
    node.node_type = "column";
    node.properties["data_type"] = data_type;
    return node;
  }

  void SetUp() override {
    system(("rm -rf " + test_dir_).c_str());
  }

  void TearDown() override {
    system(("rm -rf " + test_dir_).c_str());
  }
};

std::string GraphEngineTest::test_dir_ = "/tmp/zvec_engine_test";

// 1. CreateAndOpen — Create engine, verify schema name
TEST_F(GraphEngineTest, CreateAndOpen) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);
  EXPECT_EQ(engine->GetSchema().Name(), "test_graph");
  EXPECT_EQ(engine->GetSchema().NodeTypeCount(), 2);
  EXPECT_EQ(engine->GetSchema().EdgeTypeCount(), 1);
}

// 2. OpenExisting — Create, add a node, close, reopen, verify persistence
TEST_F(GraphEngineTest, OpenExisting) {
  auto schema = MakeTestSchema();
  {
    auto engine = GraphEngine::Create(test_dir_, schema);
    ASSERT_NE(engine, nullptr);

    auto s = engine->AddNode(MakeTableNode("t1", "prod"));
    EXPECT_TRUE(s.ok()) << s.message();
    // engine goes out of scope — unique_ptr destroyed
  }

  // Reopen
  auto engine = GraphEngine::Open(test_dir_);
  ASSERT_NE(engine, nullptr);
  EXPECT_EQ(engine->GetSchema().Name(), "test_graph");

  auto nodes = engine->FetchNodes({"t1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_EQ(nodes[0].id, "t1");
  EXPECT_EQ(nodes[0].node_type, "table");
  EXPECT_EQ(nodes[0].properties.at("database"), "prod");
}

// 3. EndToEndTraversal — Create, add 2 nodes + 1 edge, traverse, verify
TEST_F(GraphEngineTest, EndToEndTraversal) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);

  EXPECT_TRUE(engine->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(engine->AddNode(MakeColumnNode("c1", "int")).ok());
  EXPECT_TRUE(engine->AddEdge("t1", "c1", "has_column", {}).ok());

  TraversalParams params;
  params.start_ids = {"t1"};
  params.max_depth = 1;

  Subgraph sg = engine->Traverse(params);
  EXPECT_EQ(sg.nodes.size(), 2);
  EXPECT_EQ(sg.edges.size(), 1);

  // Verify ToText() produces non-empty output
  std::string text = sg.ToText();
  EXPECT_FALSE(text.empty());
  EXPECT_NE(text.find("t1"), std::string::npos);
  EXPECT_NE(text.find("c1"), std::string::npos);
}

// 4. Destroy — Create, destroy, verify Open returns nullptr
TEST_F(GraphEngineTest, Destroy) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);

  engine->Destroy();

  auto reopened = GraphEngine::Open(test_dir_);
  EXPECT_EQ(reopened, nullptr);
}

// 5. Repair — Create, add node, call Repair(), verify OK status
TEST_F(GraphEngineTest, Repair) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);

  EXPECT_TRUE(engine->AddNode(MakeTableNode("t1", "prod")).ok());

  auto s = engine->Repair();
  EXPECT_TRUE(s.ok()) << s.message();
}

// 6. CreateDuplicateFails — Create at path, try Create again at same path
TEST_F(GraphEngineTest, CreateDuplicateFails) {
  auto schema = MakeTestSchema();
  auto engine1 = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine1, nullptr);

  auto engine2 = GraphEngine::Create(test_dir_, schema);
  EXPECT_EQ(engine2, nullptr);
}

// 7. OpenNonexistentFails — Open at non-existent path returns nullptr
TEST_F(GraphEngineTest, OpenNonexistentFails) {
  auto engine = GraphEngine::Open("/tmp/zvec_engine_test_nonexistent");
  EXPECT_EQ(engine, nullptr);
}

// 8. AddNodeThroughEngine — Verify AddNode delegates properly
TEST_F(GraphEngineTest, AddNodeThroughEngine) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);

  // Valid node
  auto s = engine->AddNode(MakeTableNode("t1", "prod"));
  EXPECT_TRUE(s.ok()) << s.message();

  // Fetch it back
  auto nodes = engine->FetchNodes({"t1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_EQ(nodes[0].id, "t1");

  // Invalid node type — should fail schema validation
  GraphNode bad;
  bad.id = "bad1";
  bad.node_type = "nonexistent_type";
  s = engine->AddNode(bad);
  EXPECT_FALSE(s.ok());
}

// 9. AddEdgeThroughEngine — Verify AddEdge + adjacency updates work
TEST_F(GraphEngineTest, AddEdgeThroughEngine) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);

  EXPECT_TRUE(engine->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(engine->AddNode(MakeColumnNode("c1", "int")).ok());

  auto s = engine->AddEdge("t1", "c1", "has_column", {});
  EXPECT_TRUE(s.ok()) << s.message();

  // Verify the edge exists
  std::string edge_id = "t1--has_column--c1";
  auto edges = engine->FetchEdges({edge_id});
  ASSERT_EQ(edges.size(), 1);
  EXPECT_EQ(edges[0].source_id, "t1");
  EXPECT_EQ(edges[0].target_id, "c1");
  EXPECT_EQ(edges[0].edge_type, "has_column");

  // Verify adjacency on both nodes
  auto nodes = engine->FetchNodes({"t1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_FALSE(nodes[0].neighbor_ids.empty());
  EXPECT_EQ(nodes[0].neighbor_ids[0], "c1");

  nodes = engine->FetchNodes({"c1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_FALSE(nodes[0].neighbor_ids.empty());
  EXPECT_EQ(nodes[0].neighbor_ids[0], "t1");
}

// 10. RemoveNodeThroughEngine — Add node + edge, remove node, verify cascade
TEST_F(GraphEngineTest, RemoveNodeThroughEngine) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);

  EXPECT_TRUE(engine->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(engine->AddNode(MakeColumnNode("c1", "int")).ok());
  EXPECT_TRUE(engine->AddEdge("t1", "c1", "has_column", {}).ok());

  // Remove the table node — should cascade-delete the edge
  auto s = engine->RemoveNode("t1");
  EXPECT_TRUE(s.ok()) << s.message();

  // Node should be gone
  auto nodes = engine->FetchNodes({"t1"});
  EXPECT_TRUE(nodes.empty());

  // Edge should be gone
  std::string edge_id = "t1--has_column--c1";
  auto edges = engine->FetchEdges({edge_id});
  EXPECT_TRUE(edges.empty());

  // Column node should still exist but have clean adjacency
  nodes = engine->FetchNodes({"c1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_TRUE(nodes[0].neighbor_ids.empty());
  EXPECT_TRUE(nodes[0].neighbor_edge_ids.empty());
}
