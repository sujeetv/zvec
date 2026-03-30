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
#include <string>
#include <vector>

#include "graph/graph_schema.h"
#include "graph/storage/storage_interface.h"
#include "graph/storage/zvec_storage.h"
#include <zvec/ailego/utility/file_helper.h>

using namespace zvec;
using namespace zvec::graph;

static const std::string kTestPath = "/tmp/zvec_storage_test";

class ZvecStorageTest : public testing::Test {
 protected:
  void SetUp() override {
    ailego::FileHelper::RemoveDirectory(kTestPath.c_str());
    storage_ = ZvecStorage::Create(kTestPath, MakeSchema());
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      storage_->Destroy();
      storage_.reset();
    }
    ailego::FileHelper::RemoveDirectory(kTestPath.c_str());
  }

  //! Build a graph schema with a "table" node type, "column" node type,
  //! and a "has_column" edge type for testing.
  static GraphSchema MakeSchema() {
    GraphSchema schema("test_graph");

    auto table_type = NodeTypeBuilder("table")
                          .AddProperty("database", zvec::proto::DT_STRING, false)
                          .AddProperty("row_count", zvec::proto::DT_STRING, true)
                          .AddVector("embedding", zvec::proto::DT_VECTOR_FP32, 4)
                          .Build();
    schema.AddNodeType(table_type);

    auto column_type = NodeTypeBuilder("column")
                           .AddProperty("data_type", zvec::proto::DT_STRING, false)
                           .Build();
    schema.AddNodeType(column_type);

    auto edge_type = EdgeTypeBuilder("has_column", true)
                         .AddProperty("ordinal", zvec::proto::DT_STRING, true)
                         .Build();
    schema.AddEdgeType(edge_type);

    return schema;
  }

  //! Helper to make a basic node.
  static GraphNode MakeNode(const std::string& id,
                            const std::string& node_type,
                            const std::unordered_map<std::string, std::string>& props = {}) {
    GraphNode n;
    n.id = id;
    n.node_type = node_type;
    n.properties = props;
    return n;
  }

  //! Helper to make a basic edge.
  static GraphEdge MakeEdge(const std::string& id,
                            const std::string& source_id,
                            const std::string& target_id,
                            const std::string& edge_type,
                            bool directed = true,
                            const std::unordered_map<std::string, std::string>& props = {}) {
    GraphEdge e;
    e.id = id;
    e.source_id = source_id;
    e.target_id = target_id;
    e.edge_type = edge_type;
    e.directed = directed;
    e.properties = props;
    return e;
  }

  std::unique_ptr<ZvecStorage> storage_;
};

// ---------------------------------------------------------------------------
// 1. UpsertAndFetchNode
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, UpsertAndFetchNode) {
  GraphNode node = MakeNode("orders", "table", {{"database", "analytics"}});

  auto s = storage_->UpsertNodes({node});
  ASSERT_TRUE(s.ok()) << s.message();

  auto result = storage_->FetchNodes({"orders"});
  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(result.value().size(), 1);

  const auto& fetched = result.value()[0];
  EXPECT_EQ(fetched.id, "orders");
  EXPECT_EQ(fetched.node_type, "table");
  EXPECT_EQ(fetched.properties.at("database"), "analytics");
}

// ---------------------------------------------------------------------------
// 2. UpsertAndFetchEdge
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, UpsertAndFetchEdge) {
  GraphEdge edge = MakeEdge("e1", "orders", "orders.cid", "has_column");

  auto s = storage_->UpsertEdges({edge});
  ASSERT_TRUE(s.ok()) << s.message();

  auto result = storage_->FetchEdges({"e1"});
  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(result.value().size(), 1);

  const auto& fetched = result.value()[0];
  EXPECT_EQ(fetched.id, "e1");
  EXPECT_EQ(fetched.source_id, "orders");
  EXPECT_EQ(fetched.target_id, "orders.cid");
  EXPECT_EQ(fetched.edge_type, "has_column");
  EXPECT_TRUE(fetched.directed);
}

// ---------------------------------------------------------------------------
// 3. UpdateNodeProperties
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, UpdateNodeProperties) {
  GraphNode node = MakeNode("n1", "table", {{"database", "prod"}});
  ASSERT_TRUE(storage_->UpsertNodes({node}).ok());

  // Update the property value.
  node.properties["database"] = "staging";
  ASSERT_TRUE(storage_->UpsertNodes({node}).ok());

  auto result = storage_->FetchNodes({"n1"});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().size(), 1);
  EXPECT_EQ(result.value()[0].properties.at("database"), "staging");
}

// ---------------------------------------------------------------------------
// 4. DeleteNode
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, DeleteNode) {
  GraphNode node = MakeNode("n1", "column", {{"data_type", "INT64"}});
  ASSERT_TRUE(storage_->UpsertNodes({node}).ok());

  auto s = storage_->DeleteNodes({"n1"});
  ASSERT_TRUE(s.ok()) << s.message();

  auto result = storage_->FetchNodes({"n1"});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result.value().empty());
}

// ---------------------------------------------------------------------------
// 5. DeleteEdge
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, DeleteEdge) {
  GraphEdge edge = MakeEdge("e1", "a", "b", "has_column");
  ASSERT_TRUE(storage_->UpsertEdges({edge}).ok());

  auto s = storage_->DeleteEdges({"e1"});
  ASSERT_TRUE(s.ok()) << s.message();

  auto result = storage_->FetchEdges({"e1"});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result.value().empty());
}

// ---------------------------------------------------------------------------
// 6. DeleteNonexistentSucceeds
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, DeleteNonexistentSucceeds) {
  auto s = storage_->DeleteNodes({"no_such_node"});
  EXPECT_TRUE(s.ok()) << s.message();

  s = storage_->DeleteEdges({"no_such_edge"});
  EXPECT_TRUE(s.ok()) << s.message();
}

// ---------------------------------------------------------------------------
// 7. FetchMultipleNodes
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, FetchMultipleNodes) {
  GraphNode n1 = MakeNode("n1", "table", {{"database", "a"}});
  GraphNode n2 = MakeNode("n2", "table", {{"database", "b"}});
  GraphNode n3 = MakeNode("n3", "column", {{"data_type", "STRING"}});

  ASSERT_TRUE(storage_->UpsertNodes({n1, n2, n3}).ok());

  auto result = storage_->FetchNodes({"n1", "n2", "n3"});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 3);

  // Verify all IDs are present.
  std::vector<std::string> ids;
  for (const auto& n : result.value()) {
    ids.push_back(n.id);
  }
  std::sort(ids.begin(), ids.end());
  EXPECT_EQ(ids, (std::vector<std::string>{"n1", "n2", "n3"}));
}

// ---------------------------------------------------------------------------
// 8. FetchNonexistentReturnsEmpty
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, FetchNonexistentReturnsEmpty) {
  auto result = storage_->FetchNodes({"ghost1", "ghost2"});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result.value().empty());
}

// ---------------------------------------------------------------------------
// 9. FilterEdgesBySourceId
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, FilterEdgesBySourceId) {
  GraphEdge e1 = MakeEdge("e1", "orders", "orders.cid", "has_column");
  GraphEdge e2 = MakeEdge("e2", "orders", "orders.total", "has_column");
  GraphEdge e3 = MakeEdge("e3", "users", "users.name", "has_column");

  ASSERT_TRUE(storage_->UpsertEdges({e1, e2, e3}).ok());

  // Create a scalar index on source_id so filter queries work.
  auto idx_s = storage_->CreateIndex("edges", "source_id");
  ASSERT_TRUE(idx_s.ok()) << idx_s.message();

  auto result = storage_->FilterEdges("source_id = 'orders'");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result.value().size(), 2);

  for (const auto& e : result.value()) {
    EXPECT_EQ(e.source_id, "orders");
  }
}

// ---------------------------------------------------------------------------
// 10. FilterNodesByType
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, FilterNodesByType) {
  GraphNode n1 = MakeNode("n1", "table", {{"database", "a"}});
  GraphNode n2 = MakeNode("n2", "table", {{"database", "b"}});
  GraphNode n3 = MakeNode("n3", "column", {{"data_type", "INT64"}});

  ASSERT_TRUE(storage_->UpsertNodes({n1, n2, n3}).ok());

  // Create a scalar index on node_type so filter queries work.
  auto idx_s = storage_->CreateIndex("nodes", "node_type");
  ASSERT_TRUE(idx_s.ok()) << idx_s.message();

  auto result = storage_->FilterNodes("node_type = 'table'");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result.value().size(), 2);

  for (const auto& n : result.value()) {
    EXPECT_EQ(n.node_type, "table");
  }
}

// ---------------------------------------------------------------------------
// 11. UpsertNodeWithVectors
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, UpsertNodeWithVectors) {
  GraphNode node = MakeNode("v1", "table", {{"database", "vec_db"}});
  node.vectors["embedding"] = {1.0f, 2.0f, 3.0f, 4.0f};

  ASSERT_TRUE(storage_->UpsertNodes({node}).ok());

  auto result = storage_->FetchNodes({"v1"});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().size(), 1);

  const auto& fetched = result.value()[0];
  EXPECT_EQ(fetched.id, "v1");
  ASSERT_TRUE(fetched.vectors.count("embedding") > 0);

  const auto& vec = fetched.vectors.at("embedding");
  ASSERT_EQ(vec.size(), 4);
  EXPECT_FLOAT_EQ(vec[0], 1.0f);
  EXPECT_FLOAT_EQ(vec[1], 2.0f);
  EXPECT_FLOAT_EQ(vec[2], 3.0f);
  EXPECT_FLOAT_EQ(vec[3], 4.0f);
}

// ---------------------------------------------------------------------------
// 12. UpsertEdgeWithProperties
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, UpsertEdgeWithProperties) {
  GraphEdge edge = MakeEdge("e1", "orders", "orders.cid", "has_column",
                            /*directed=*/true, {{"ordinal", "1"}});

  ASSERT_TRUE(storage_->UpsertEdges({edge}).ok());

  auto result = storage_->FetchEdges({"e1"});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().size(), 1);

  const auto& fetched = result.value()[0];
  EXPECT_EQ(fetched.properties.at("ordinal"), "1");
}

// ---------------------------------------------------------------------------
// 13. AtomicBatchUpserts
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, AtomicBatchUpserts) {
  GraphNode n1 = MakeNode("n1", "table", {{"database", "a"}});
  GraphNode n2 = MakeNode("n2", "column", {{"data_type", "INT64"}});
  GraphEdge e1 = MakeEdge("e1", "n1", "n2", "has_column");

  std::vector<Mutation> mutations = {
      Mutation::UpsertNode(n1),
      Mutation::UpsertNode(n2),
      Mutation::UpsertEdge(e1),
  };

  auto s = storage_->AtomicBatch(mutations);
  ASSERT_TRUE(s.ok()) << s.message();

  // Verify all were applied.
  auto nr = storage_->FetchNodes({"n1", "n2"});
  ASSERT_TRUE(nr.has_value());
  EXPECT_EQ(nr.value().size(), 2);

  auto er = storage_->FetchEdges({"e1"});
  ASSERT_TRUE(er.has_value());
  EXPECT_EQ(er.value().size(), 1);
}

// ---------------------------------------------------------------------------
// 14. AtomicBatchMixedOperations
// ---------------------------------------------------------------------------

TEST_F(ZvecStorageTest, AtomicBatchMixedOperations) {
  // Pre-populate: insert two nodes and one edge.
  GraphNode n1 = MakeNode("n1", "table", {{"database", "a"}});
  GraphNode n2 = MakeNode("n2", "column", {{"data_type", "INT64"}});
  GraphEdge e1 = MakeEdge("e1", "n1", "n2", "has_column");

  ASSERT_TRUE(storage_->UpsertNodes({n1, n2}).ok());
  ASSERT_TRUE(storage_->UpsertEdges({e1}).ok());

  // Now batch: delete n2 and e1, add n3.
  GraphNode n3 = MakeNode("n3", "table", {{"database", "new"}});
  GraphNode n2_for_delete;
  n2_for_delete.id = "n2";
  GraphEdge e1_for_delete;
  e1_for_delete.id = "e1";

  std::vector<Mutation> mutations = {
      Mutation::DeleteNode(n2_for_delete),
      Mutation::DeleteEdge(e1_for_delete),
      Mutation::UpsertNode(n3),
  };

  auto s = storage_->AtomicBatch(mutations);
  ASSERT_TRUE(s.ok()) << s.message();

  // n1 should still be there.
  auto nr1 = storage_->FetchNodes({"n1"});
  ASSERT_TRUE(nr1.has_value());
  EXPECT_EQ(nr1.value().size(), 1);

  // n2 should be gone.
  auto nr2 = storage_->FetchNodes({"n2"});
  ASSERT_TRUE(nr2.has_value());
  EXPECT_TRUE(nr2.value().empty());

  // n3 should exist.
  auto nr3 = storage_->FetchNodes({"n3"});
  ASSERT_TRUE(nr3.has_value());
  EXPECT_EQ(nr3.value().size(), 1);
  EXPECT_EQ(nr3.value()[0].properties.at("database"), "new");

  // e1 should be gone.
  auto er = storage_->FetchEdges({"e1"});
  ASSERT_TRUE(er.has_value());
  EXPECT_TRUE(er.value().empty());
}
