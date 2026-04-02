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

#include "graph/graph_collection.h"

using namespace zvec;
using namespace zvec::graph;

class GraphCollectionTest : public testing::Test {
 protected:
  static constexpr const char* kTestDir = "/tmp/zvec_graph_collection_test";

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
    std::filesystem::remove_all(kTestDir);
  }

  void TearDown() override {
    std::filesystem::remove_all(kTestDir);
  }
};

// 1. Create and verify schema
TEST_F(GraphCollectionTest, CreateAndOpen) {
  auto schema = MakeTestSchema();
  auto gc = GraphCollection::Create(kTestDir, schema);
  ASSERT_NE(gc, nullptr);
  EXPECT_EQ(gc->GetSchema().Name(), "test_graph");
  EXPECT_EQ(gc->GetSchema().NodeTypeCount(), 2);
  EXPECT_EQ(gc->GetSchema().EdgeTypeCount(), 1);
}

// 2. Persistence: create, add node, close, reopen, verify
TEST_F(GraphCollectionTest, OpenExisting) {
  auto schema = MakeTestSchema();
  {
    auto gc = GraphCollection::Create(kTestDir, schema);
    ASSERT_NE(gc, nullptr);
    EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
    EXPECT_TRUE(gc->Flush().ok());
  }

  auto gc = GraphCollection::Open(kTestDir);
  ASSERT_NE(gc, nullptr);
  EXPECT_EQ(gc->GetSchema().Name(), "test_graph");

  auto nodes = gc->FetchNodes({"t1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_EQ(nodes[0].id, "t1");
  EXPECT_EQ(nodes[0].node_type, "table");
  EXPECT_EQ(nodes[0].properties.at("database"), "prod");
}

// 3. End-to-end traversal
TEST_F(GraphCollectionTest, EndToEndTraversal) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->AddNode(MakeColumnNode("c1", "int")).ok());
  EXPECT_TRUE(gc->AddEdge("t1", "c1", "has_column", {}).ok());

  TraversalParams params;
  params.start_ids = {"t1"};
  params.max_depth = 1;

  Subgraph sg = gc->Traverse(params);
  EXPECT_EQ(sg.nodes.size(), 2);
  EXPECT_EQ(sg.edges.size(), 1);

  std::string text = sg.ToText();
  EXPECT_FALSE(text.empty());
  EXPECT_NE(text.find("t1"), std::string::npos);
  EXPECT_NE(text.find("c1"), std::string::npos);
}

// 4. Destroy
TEST_F(GraphCollectionTest, Destroy) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);
  gc->Destroy();

  auto reopened = GraphCollection::Open(kTestDir);
  EXPECT_EQ(reopened, nullptr);
}

// 5. Duplicate create fails
TEST_F(GraphCollectionTest, CreateDuplicateFails) {
  auto schema = MakeTestSchema();
  auto gc1 = GraphCollection::Create(kTestDir, schema);
  ASSERT_NE(gc1, nullptr);
  auto gc2 = GraphCollection::Create(kTestDir, schema);
  EXPECT_EQ(gc2, nullptr);
}

// 6. Open nonexistent fails
TEST_F(GraphCollectionTest, OpenNonexistentFails) {
  auto gc = GraphCollection::Open("/tmp/zvec_gc_nonexistent");
  EXPECT_EQ(gc, nullptr);
}

// 7. Add node delegates to mutation engine
TEST_F(GraphCollectionTest, AddNodeThroughCollection) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());

  auto nodes = gc->FetchNodes({"t1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_EQ(nodes[0].id, "t1");

  // Invalid type should fail
  GraphNode bad;
  bad.id = "bad1";
  bad.node_type = "nonexistent_type";
  EXPECT_FALSE(gc->AddNode(bad).ok());
}

// 8. Add edge + verify adjacency
TEST_F(GraphCollectionTest, AddEdgeThroughCollection) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->AddNode(MakeColumnNode("c1", "int")).ok());
  EXPECT_TRUE(gc->AddEdge("t1", "c1", "has_column", {}).ok());

  std::string edge_id = "t1--has_column--c1";
  auto edges = gc->FetchEdges({edge_id});
  ASSERT_EQ(edges.size(), 1);
  EXPECT_EQ(edges[0].source_id, "t1");
  EXPECT_EQ(edges[0].target_id, "c1");

  auto nodes = gc->FetchNodes({"t1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_FALSE(nodes[0].neighbor_ids.empty());
  EXPECT_EQ(nodes[0].neighbor_ids[0], "c1");
}

// 9. Remove node cascades edges
TEST_F(GraphCollectionTest, RemoveNodeCascade) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->AddNode(MakeColumnNode("c1", "int")).ok());
  EXPECT_TRUE(gc->AddEdge("t1", "c1", "has_column", {}).ok());

  EXPECT_TRUE(gc->RemoveNode("t1").ok());

  EXPECT_TRUE(gc->FetchNodes({"t1"}).empty());
  EXPECT_TRUE(gc->FetchEdges({"t1--has_column--c1"}).empty());

  auto nodes = gc->FetchNodes({"c1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_TRUE(nodes[0].neighbor_ids.empty());
  EXPECT_TRUE(nodes[0].neighbor_edge_ids.empty());
}

// 10. Filter nodes by type
TEST_F(GraphCollectionTest, FilterNodesByType) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->AddNode(MakeTableNode("t2", "staging")).ok());
  EXPECT_TRUE(gc->AddNode(MakeColumnNode("c1", "int")).ok());

  auto tables = gc->FilterNodes("node_type = 'table'");
  EXPECT_EQ(tables.size(), 2);
  for (const auto& node : tables) {
    EXPECT_EQ(node.node_type, "table");
  }
}

// 11. Filter edges
TEST_F(GraphCollectionTest, FilterEdgesByType) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->AddNode(MakeColumnNode("c1", "int")).ok());
  EXPECT_TRUE(gc->AddNode(MakeColumnNode("c2", "varchar")).ok());
  EXPECT_TRUE(gc->AddEdge("t1", "c1", "has_column", {}).ok());
  EXPECT_TRUE(gc->AddEdge("t1", "c2", "has_column", {}).ok());

  auto edges = gc->FilterEdges("edge_type = 'has_column'");
  EXPECT_EQ(edges.size(), 2);
}

// 12. User-defined index
TEST_F(GraphCollectionTest, CreateUserIndex) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->AddNode(MakeTableNode("t2", "staging")).ok());
  EXPECT_TRUE(gc->AddNode(MakeTableNode("t3", "prod")).ok());

  EXPECT_TRUE(gc->CreateIndex("nodes", "database").ok());

  auto nodes = gc->FilterNodes("database = 'prod'");
  EXPECT_EQ(nodes.size(), 2);
}

// 13. Update node properties
TEST_F(GraphCollectionTest, UpdateNode) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->UpdateNode("t1", {{"database", "staging"}}).ok());

  auto nodes = gc->FetchNodes({"t1"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_EQ(nodes[0].properties.at("database"), "staging");
}

// 14. Update edge properties
TEST_F(GraphCollectionTest, UpdateEdge) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);

  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->AddNode(MakeColumnNode("c1", "int")).ok());
  EXPECT_TRUE(gc->AddEdge("t1", "c1", "has_column", {{"weight", "1.0"}}).ok());

  EXPECT_TRUE(gc->UpdateEdge("t1--has_column--c1", {{"weight", "2.0"}}).ok());

  auto edges = gc->FetchEdges({"t1--has_column--c1"});
  ASSERT_EQ(edges.size(), 1);
  EXPECT_EQ(edges[0].properties.at("weight"), "2.0");
}

// 15. Flush
TEST_F(GraphCollectionTest, Flush) {
  auto gc = GraphCollection::Create(kTestDir, MakeTestSchema());
  ASSERT_NE(gc, nullptr);
  EXPECT_TRUE(gc->AddNode(MakeTableNode("t1", "prod")).ok());
  EXPECT_TRUE(gc->Flush().ok());
}
