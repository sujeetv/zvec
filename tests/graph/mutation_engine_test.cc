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

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "graph/graph_schema.h"
#include "graph/mutation_engine.h"
#include "graph/storage/zvec_storage.h"
#include <zvec/ailego/utility/file_helper.h>

using namespace zvec;
using namespace zvec::graph;

class MutationEngineTest : public testing::Test {
 protected:
  static std::string test_dir_;
  std::unique_ptr<ZvecStorage> storage_;
  std::unique_ptr<MutationEngine> mutation_;
  GraphSchema schema_;

  MutationEngineTest() : schema_("test_graph") {}

  void SetUp() override {
    ailego::FileHelper::RemoveDirectory(test_dir_.c_str());

    NodeTypeBuilder nb("table");
    nb.AddProperty("database", zvec::proto::DT_STRING, false);
    schema_.AddNodeType(nb.Build());

    NodeTypeBuilder nb2("column");
    nb2.AddProperty("data_type", zvec::proto::DT_STRING, false);
    schema_.AddNodeType(nb2.Build());

    EdgeTypeBuilder eb("has_column", true);
    schema_.AddEdgeType(eb.Build());
    schema_.AddEdgeConstraint("has_column", "table", "column");

    EdgeTypeBuilder eb2("learned", false);
    eb2.AddProperty("confidence", zvec::proto::DT_DOUBLE, false);
    schema_.AddEdgeType(eb2.Build());

    storage_ = ZvecStorage::Create(test_dir_, schema_);
    ASSERT_NE(storage_, nullptr);
    mutation_ = std::make_unique<MutationEngine>(&schema_, storage_.get());
  }

  void TearDown() override {
    mutation_.reset();
    if (storage_) {
      storage_->Destroy();
      storage_.reset();
    }
    ailego::FileHelper::RemoveDirectory(test_dir_.c_str());
  }

  //! Helper: create and add a table node
  GraphNode MakeTableNode(const std::string& id,
                          const std::string& database) {
    GraphNode node;
    node.id = id;
    node.node_type = "table";
    node.properties["database"] = database;
    return node;
  }

  //! Helper: create and add a column node
  GraphNode MakeColumnNode(const std::string& id,
                           const std::string& data_type) {
    GraphNode node;
    node.id = id;
    node.node_type = "column";
    node.properties["data_type"] = data_type;
    return node;
  }
};

std::string MutationEngineTest::test_dir_ = "/tmp/zvec_mutation_test";

// 1. AddNode — add valid node, verify fields
TEST_F(MutationEngineTest, AddNode) {
  GraphNode node = MakeTableNode("t1", "mydb");
  Status s = mutation_->AddNode(node);
  ASSERT_TRUE(s.ok()) << s.message();

  auto result = storage_->FetchNodes({"t1"});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().size(), 1u);

  const auto& fetched = result.value()[0];
  EXPECT_EQ(fetched.id, "t1");
  EXPECT_EQ(fetched.node_type, "table");
  EXPECT_EQ(fetched.properties.at("database"), "mydb");
  EXPECT_GT(fetched.version, 0u);
  EXPECT_GT(fetched.updated_at, 0u);
}

// 2. AddNodeInvalidTypeRejected — node with unknown type fails
TEST_F(MutationEngineTest, AddNodeInvalidTypeRejected) {
  GraphNode node;
  node.id = "x1";
  node.node_type = "nonexistent_type";
  Status s = mutation_->AddNode(node);
  EXPECT_FALSE(s.ok());
}

// 3. AddEdgeUpdatesAdjacencyLists
TEST_F(MutationEngineTest, AddEdgeUpdatesAdjacencyLists) {
  ASSERT_TRUE(mutation_->AddNode(MakeTableNode("t1", "db1")).ok());
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c1", "int")).ok());

  Status s = mutation_->AddEdge("t1", "c1", "has_column", {});
  ASSERT_TRUE(s.ok()) << s.message();

  // Verify source node adjacency
  auto src = storage_->FetchNodes({"t1"});
  ASSERT_TRUE(src.has_value());
  ASSERT_EQ(src.value().size(), 1u);
  EXPECT_EQ(src.value()[0].neighbor_ids.size(), 1u);
  EXPECT_EQ(src.value()[0].neighbor_ids[0], "c1");
  EXPECT_EQ(src.value()[0].neighbor_edge_ids.size(), 1u);
  EXPECT_EQ(src.value()[0].neighbor_edge_ids[0], "t1--has_column--c1");

  // Verify target node adjacency
  auto tgt = storage_->FetchNodes({"c1"});
  ASSERT_TRUE(tgt.has_value());
  ASSERT_EQ(tgt.value().size(), 1u);
  EXPECT_EQ(tgt.value()[0].neighbor_ids.size(), 1u);
  EXPECT_EQ(tgt.value()[0].neighbor_ids[0], "t1");
  EXPECT_EQ(tgt.value()[0].neighbor_edge_ids.size(), 1u);

  // Verify edge document
  auto edge = storage_->FetchEdges({"t1--has_column--c1"});
  ASSERT_TRUE(edge.has_value());
  ASSERT_EQ(edge.value().size(), 1u);
  EXPECT_EQ(edge.value()[0].source_id, "t1");
  EXPECT_EQ(edge.value()[0].target_id, "c1");
  EXPECT_EQ(edge.value()[0].edge_type, "has_column");
}

// 4. AddEdgeConstraintViolationRejected
TEST_F(MutationEngineTest, AddEdgeConstraintViolationRejected) {
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c1", "int")).ok());
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c2", "varchar")).ok());

  // has_column only allows table -> column, not column -> column
  Status s = mutation_->AddEdge("c1", "c2", "has_column", {});
  EXPECT_FALSE(s.ok());
}

// 5. AddEdgeMissingSourceNodeRejected
TEST_F(MutationEngineTest, AddEdgeMissingSourceNodeRejected) {
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c1", "int")).ok());

  Status s = mutation_->AddEdge("nonexistent", "c1", "has_column", {});
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.code(), StatusCode::NOT_FOUND);
}

// 6. AddEdgeMissingTargetNodeRejected
TEST_F(MutationEngineTest, AddEdgeMissingTargetNodeRejected) {
  ASSERT_TRUE(mutation_->AddNode(MakeTableNode("t1", "db1")).ok());

  Status s = mutation_->AddEdge("t1", "nonexistent", "has_column", {});
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.code(), StatusCode::NOT_FOUND);
}

// 7. AddEdgeIdempotent — adding same edge twice succeeds, one entry
TEST_F(MutationEngineTest, AddEdgeIdempotent) {
  ASSERT_TRUE(mutation_->AddNode(MakeTableNode("t1", "db1")).ok());
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c1", "int")).ok());

  ASSERT_TRUE(mutation_->AddEdge("t1", "c1", "has_column", {}).ok());
  ASSERT_TRUE(mutation_->AddEdge("t1", "c1", "has_column", {}).ok());

  auto src = storage_->FetchNodes({"t1"});
  ASSERT_TRUE(src.has_value());
  ASSERT_EQ(src.value().size(), 1u);
  // Should only have one entry, not two
  EXPECT_EQ(src.value()[0].neighbor_edge_ids.size(), 1u);
  EXPECT_EQ(src.value()[0].neighbor_ids.size(), 1u);
}

// 8. RemoveEdgeCleansUpAdjacency
TEST_F(MutationEngineTest, RemoveEdgeCleansUpAdjacency) {
  ASSERT_TRUE(mutation_->AddNode(MakeTableNode("t1", "db1")).ok());
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c1", "int")).ok());
  ASSERT_TRUE(mutation_->AddEdge("t1", "c1", "has_column", {}).ok());

  Status s = mutation_->RemoveEdge("t1--has_column--c1");
  ASSERT_TRUE(s.ok()) << s.message();

  // Both nodes should have empty adjacency
  auto src = storage_->FetchNodes({"t1"});
  ASSERT_TRUE(src.has_value());
  ASSERT_EQ(src.value().size(), 1u);
  EXPECT_TRUE(src.value()[0].neighbor_ids.empty());
  EXPECT_TRUE(src.value()[0].neighbor_edge_ids.empty());

  auto tgt = storage_->FetchNodes({"c1"});
  ASSERT_TRUE(tgt.has_value());
  ASSERT_EQ(tgt.value().size(), 1u);
  EXPECT_TRUE(tgt.value()[0].neighbor_ids.empty());
  EXPECT_TRUE(tgt.value()[0].neighbor_edge_ids.empty());

  // Edge should be gone
  auto edge = storage_->FetchEdges({"t1--has_column--c1"});
  ASSERT_TRUE(edge.has_value());
  EXPECT_TRUE(edge.value().empty());
}

// 9. RemoveNodeCascadesEdges
TEST_F(MutationEngineTest, RemoveNodeCascadesEdges) {
  ASSERT_TRUE(mutation_->AddNode(MakeTableNode("t1", "db1")).ok());
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c1", "int")).ok());
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c2", "varchar")).ok());
  ASSERT_TRUE(mutation_->AddEdge("t1", "c1", "has_column", {}).ok());
  ASSERT_TRUE(mutation_->AddEdge("t1", "c2", "has_column", {}).ok());

  Status s = mutation_->RemoveNode("t1");
  ASSERT_TRUE(s.ok()) << s.message();

  // Node should be gone
  auto node = storage_->FetchNodes({"t1"});
  ASSERT_TRUE(node.has_value());
  EXPECT_TRUE(node.value().empty());

  // Edges should be gone
  auto e1 = storage_->FetchEdges({"t1--has_column--c1"});
  ASSERT_TRUE(e1.has_value());
  EXPECT_TRUE(e1.value().empty());

  auto e2 = storage_->FetchEdges({"t1--has_column--c2"});
  ASSERT_TRUE(e2.has_value());
  EXPECT_TRUE(e2.value().empty());

  // Connected nodes' adjacency should be cleaned
  auto c1 = storage_->FetchNodes({"c1"});
  ASSERT_TRUE(c1.has_value());
  ASSERT_EQ(c1.value().size(), 1u);
  EXPECT_TRUE(c1.value()[0].neighbor_ids.empty());
  EXPECT_TRUE(c1.value()[0].neighbor_edge_ids.empty());

  auto c2 = storage_->FetchNodes({"c2"});
  ASSERT_TRUE(c2.has_value());
  ASSERT_EQ(c2.value().size(), 1u);
  EXPECT_TRUE(c2.value()[0].neighbor_ids.empty());
  EXPECT_TRUE(c2.value()[0].neighbor_edge_ids.empty());
}

// 10. RemoveNonexistentNodeSucceeds
TEST_F(MutationEngineTest, RemoveNonexistentNodeSucceeds) {
  Status s = mutation_->RemoveNode("does_not_exist");
  EXPECT_TRUE(s.ok());
}

// 11. UpdateNodeProperties — verify merge
TEST_F(MutationEngineTest, UpdateNodeProperties) {
  ASSERT_TRUE(mutation_->AddNode(MakeTableNode("t1", "db1")).ok());

  Status s = mutation_->UpdateNode("t1", {{"database", "db2"}});
  ASSERT_TRUE(s.ok()) << s.message();

  auto result = storage_->FetchNodes({"t1"});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().size(), 1u);

  const auto& props = result.value()[0].properties;
  EXPECT_EQ(props.at("database"), "db2");
}

// 12. UpdateNodeBumpsVersion
TEST_F(MutationEngineTest, UpdateNodeBumpsVersion) {
  ASSERT_TRUE(mutation_->AddNode(MakeTableNode("t1", "db1")).ok());

  auto before = storage_->FetchNodes({"t1"});
  ASSERT_TRUE(before.has_value());
  uint64_t v1 = before.value()[0].version;

  ASSERT_TRUE(
      mutation_->UpdateNode("t1", {{"database", "db2"}}).ok());

  auto after = storage_->FetchNodes({"t1"});
  ASSERT_TRUE(after.has_value());
  uint64_t v2 = after.value()[0].version;

  EXPECT_GT(v2, v1);
}

// 13. AddEdgeWithProperties
TEST_F(MutationEngineTest, AddEdgeWithProperties) {
  ASSERT_TRUE(mutation_->AddNode(MakeTableNode("t1", "db1")).ok());
  ASSERT_TRUE(mutation_->AddNode(MakeColumnNode("c1", "int")).ok());

  // "learned" edge type has no constraints, so table->column is allowed
  std::unordered_map<std::string, std::string> props = {
      {"confidence", "0.95"}};
  Status s = mutation_->AddEdge("t1", "c1", "learned", props);
  ASSERT_TRUE(s.ok()) << s.message();

  auto edge = storage_->FetchEdges({"t1--learned--c1"});
  ASSERT_TRUE(edge.has_value());
  ASSERT_EQ(edge.value().size(), 1u);
  EXPECT_EQ(edge.value()[0].properties.at("confidence"), "0.95");
  EXPECT_EQ(edge.value()[0].edge_type, "learned");
}
