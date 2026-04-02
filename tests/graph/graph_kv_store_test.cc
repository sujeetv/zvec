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
#include <thread>
#include <vector>

#include "graph/storage/graph_kv_store.h"

using namespace zvec;
using namespace zvec::graph;

class GraphKVStoreTest : public testing::Test {
 protected:
  static constexpr const char* kTestDir = "/tmp/zvec_kv_store_test";

  void SetUp() override {
    std::filesystem::remove_all(kTestDir);
  }

  void TearDown() override {
    std::filesystem::remove_all(kTestDir);
  }

  GraphNode MakeNode(const std::string& id, const std::string& type,
                     std::unordered_map<std::string, std::string> props = {}) {
    GraphNode n;
    n.id = id;
    n.node_type = type;
    n.properties = std::move(props);
    n.version = 1;
    n.updated_at = 12345;
    return n;
  }

  GraphEdge MakeEdge(const std::string& id, const std::string& src,
                     const std::string& tgt, const std::string& type) {
    GraphEdge e;
    e.id = id;
    e.source_id = src;
    e.target_id = tgt;
    e.edge_type = type;
    e.directed = true;
    e.version = 1;
    e.updated_at = 12345;
    return e;
  }
};

TEST_F(GraphKVStoreTest, CreateAndOpen) {
  {
    auto store = GraphKVStore::Create(kTestDir);
    ASSERT_NE(store, nullptr);
  }
  auto store = GraphKVStore::Open(kTestDir);
  ASSERT_NE(store, nullptr);
}

TEST_F(GraphKVStoreTest, UpsertAndFetchNodes) {
  auto store = GraphKVStore::Create(kTestDir);
  ASSERT_NE(store, nullptr);

  auto n1 = MakeNode("n1", "table", {{"db", "prod"}});
  auto n2 = MakeNode("n2", "column", {{"dtype", "int"}});

  EXPECT_TRUE(store->UpsertNodes({n1, n2}).ok());

  auto result = store->FetchNodes({"n1", "n2", "n3"});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 2);

  // Verify content
  bool found_n1 = false, found_n2 = false;
  for (const auto& node : result.value()) {
    if (node.id == "n1") {
      EXPECT_EQ(node.node_type, "table");
      EXPECT_EQ(node.properties.at("db"), "prod");
      found_n1 = true;
    } else if (node.id == "n2") {
      EXPECT_EQ(node.node_type, "column");
      found_n2 = true;
    }
  }
  EXPECT_TRUE(found_n1);
  EXPECT_TRUE(found_n2);
}

TEST_F(GraphKVStoreTest, FetchNodesLiteSkipsProperties) {
  auto store = GraphKVStore::Create(kTestDir);
  auto n1 = MakeNode("n1", "table", {{"db", "prod"}, {"schema", "public"}});
  n1.neighbor_ids = {"n2", "n3"};
  n1.neighbor_edge_ids = {"e1", "e2"};
  EXPECT_TRUE(store->UpsertNodes({n1}).ok());

  auto result = store->FetchNodesLite({"n1"});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().size(), 1);

  const auto& node = result.value()[0];
  EXPECT_EQ(node.id, "n1");
  EXPECT_EQ(node.node_type, "table");
  EXPECT_TRUE(node.properties.empty());  // Lite skips properties
  EXPECT_EQ(node.neighbor_ids.size(), 2);
  EXPECT_EQ(node.neighbor_edge_ids.size(), 2);
}

TEST_F(GraphKVStoreTest, DeleteNodes) {
  auto store = GraphKVStore::Create(kTestDir);
  EXPECT_TRUE(store->UpsertNodes({MakeNode("n1", "table")}).ok());

  EXPECT_TRUE(store->DeleteNodes({"n1"}).ok());

  auto result = store->FetchNodes({"n1"});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result.value().empty());
}

TEST_F(GraphKVStoreTest, UpsertAndFetchEdges) {
  auto store = GraphKVStore::Create(kTestDir);
  auto e1 = MakeEdge("e1", "n1", "n2", "has_column");
  e1.properties["weight"] = "0.5";

  EXPECT_TRUE(store->UpsertEdges({e1}).ok());

  auto result = store->FetchEdges({"e1"});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().size(), 1);

  const auto& edge = result.value()[0];
  EXPECT_EQ(edge.source_id, "n1");
  EXPECT_EQ(edge.target_id, "n2");
  EXPECT_EQ(edge.edge_type, "has_column");
  EXPECT_EQ(edge.properties.at("weight"), "0.5");
}

TEST_F(GraphKVStoreTest, DeleteEdges) {
  auto store = GraphKVStore::Create(kTestDir);
  EXPECT_TRUE(store->UpsertEdges({MakeEdge("e1", "n1", "n2", "rel")}).ok());
  EXPECT_TRUE(store->DeleteEdges({"e1"}).ok());

  auto result = store->FetchEdges({"e1"});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result.value().empty());
}

TEST_F(GraphKVStoreTest, AtomicBatch) {
  auto store = GraphKVStore::Create(kTestDir);

  auto n1 = MakeNode("n1", "table");
  auto e1 = MakeEdge("e1", "n1", "n2", "rel");

  std::vector<Mutation> batch;
  batch.push_back(Mutation::UpsertNode(n1));
  batch.push_back(Mutation::UpsertEdge(e1));
  EXPECT_TRUE(store->AtomicBatch(batch).ok());

  EXPECT_EQ(store->FetchNodes({"n1"}).value().size(), 1);
  EXPECT_EQ(store->FetchEdges({"e1"}).value().size(), 1);
}

TEST_F(GraphKVStoreTest, FilterNodesByType) {
  auto store = GraphKVStore::Create(kTestDir);
  EXPECT_TRUE(store->UpsertNodes({
      MakeNode("n1", "table"),
      MakeNode("n2", "column"),
      MakeNode("n3", "table"),
  }).ok());

  auto result = store->FilterNodes("node_type = 'table'");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 2);
  for (const auto& node : result.value()) {
    EXPECT_EQ(node.node_type, "table");
  }
}

TEST_F(GraphKVStoreTest, FilterEdgesByType) {
  auto store = GraphKVStore::Create(kTestDir);
  EXPECT_TRUE(store->UpsertEdges({
      MakeEdge("e1", "n1", "n2", "has_column"),
      MakeEdge("e2", "n1", "n3", "references"),
      MakeEdge("e3", "n2", "n4", "has_column"),
  }).ok());

  auto result = store->FilterEdges("edge_type = 'has_column'");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 2);
}

TEST_F(GraphKVStoreTest, FilterEdgesBySource) {
  auto store = GraphKVStore::Create(kTestDir);
  EXPECT_TRUE(store->UpsertEdges({
      MakeEdge("e1", "n1", "n2", "rel"),
      MakeEdge("e2", "n1", "n3", "rel"),
      MakeEdge("e3", "n2", "n4", "rel"),
  }).ok());

  auto result = store->FilterEdges("source_id = 'n1'");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 2);
}

TEST_F(GraphKVStoreTest, CreateUserIndex) {
  auto store = GraphKVStore::Create(kTestDir);
  EXPECT_TRUE(store->UpsertNodes({
      MakeNode("n1", "table", {{"database", "prod"}}),
      MakeNode("n2", "table", {{"database", "staging"}}),
      MakeNode("n3", "column", {{"database", "prod"}}),
  }).ok());

  // Create index — should backfill
  EXPECT_TRUE(store->CreateIndex("nodes", "database").ok());

  auto result = store->FilterNodes("database = 'prod'");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 2);
}

TEST_F(GraphKVStoreTest, DropIndex) {
  auto store = GraphKVStore::Create(kTestDir);
  EXPECT_TRUE(store->CreateIndex("nodes", "myfield").ok());
  EXPECT_TRUE(store->DropIndex("nodes", "myfield").ok());
  // Dropping again is idempotent
  EXPECT_TRUE(store->DropIndex("nodes", "myfield").ok());
}

TEST_F(GraphKVStoreTest, Persistence) {
  {
    auto store = GraphKVStore::Create(kTestDir);
    EXPECT_TRUE(store->UpsertNodes({MakeNode("n1", "table", {{"db", "prod"}})}).ok());
    EXPECT_TRUE(store->Flush().ok());
  }

  auto store = GraphKVStore::Open(kTestDir);
  ASSERT_NE(store, nullptr);
  auto result = store->FetchNodes({"n1"});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().size(), 1);
  EXPECT_EQ(result.value()[0].properties.at("db"), "prod");
}

TEST_F(GraphKVStoreTest, ConcurrentReads) {
  auto store = GraphKVStore::Create(kTestDir);

  // Insert 100 nodes
  std::vector<GraphNode> nodes;
  for (int i = 0; i < 100; ++i) {
    nodes.push_back(MakeNode("n" + std::to_string(i), "table"));
  }
  EXPECT_TRUE(store->UpsertNodes(nodes).ok());

  // Read concurrently from 8 threads
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&store, &success_count]() {
      for (int i = 0; i < 50; ++i) {
        auto result = store->FetchNodes(
            {"n0", "n10", "n20", "n30", "n40", "n50", "n60", "n70"});
        if (result.has_value() && result.value().size() == 8) {
          success_count++;
        }
      }
    });
  }

  for (auto& t : threads) t.join();
  EXPECT_EQ(success_count.load(), 8 * 50);
}

TEST_F(GraphKVStoreTest, QueryNodesReturnsError) {
  auto store = GraphKVStore::Create(kTestDir);
  auto result = store->QueryNodes("vec", {1.0f, 2.0f}, 5);
  EXPECT_FALSE(result.has_value());
}

TEST_F(GraphKVStoreTest, FlushAndDestroy) {
  auto store = GraphKVStore::Create(kTestDir);
  EXPECT_TRUE(store->UpsertNodes({MakeNode("n1", "table")}).ok());
  EXPECT_TRUE(store->Flush().ok());
  EXPECT_TRUE(store->Destroy().ok());
  EXPECT_FALSE(std::filesystem::exists(kTestDir));
}
