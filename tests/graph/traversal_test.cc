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
#include "graph/traversal.h"

using namespace zvec;
using namespace zvec::graph;

class TraversalTest : public testing::Test {
 protected:
  static std::string test_dir_;
  std::unique_ptr<ZvecStorage> storage_;
  std::unique_ptr<MutationEngine> mutation_;
  std::unique_ptr<TraversalEngine> traversal_;
  GraphSchema schema_;

  TraversalTest() : schema_("test_graph") {}

  void SetUp() override {
    system(("rm -rf " + test_dir_).c_str());

    NodeTypeBuilder nb("table");
    nb.AddProperty("database", zvec::proto::DT_STRING, false);
    schema_.AddNodeType(nb.Build());

    NodeTypeBuilder nb2("column");
    nb2.AddProperty("data_type", zvec::proto::DT_STRING, false);
    schema_.AddNodeType(nb2.Build());

    EdgeTypeBuilder eb("has_column", true);
    schema_.AddEdgeType(eb.Build());
    schema_.AddEdgeConstraint("has_column", "table", "column");

    EdgeTypeBuilder eb2("foreign_key", true);
    eb2.AddProperty("on_column", zvec::proto::DT_STRING, false);
    schema_.AddEdgeType(eb2.Build());
    schema_.AddEdgeConstraint("foreign_key", "table", "table");

    // "learned" edge type for cycle detection test
    EdgeTypeBuilder eb3("learned", false);
    schema_.AddEdgeType(eb3.Build());

    storage_ = ZvecStorage::Create(test_dir_, schema_);
    ASSERT_NE(storage_, nullptr);
    mutation_ = std::make_unique<MutationEngine>(&schema_, storage_.get());
    traversal_ = std::make_unique<TraversalEngine>(storage_.get());

    // Build graph:
    // orders (table) --has_column--> orders.customer_id (column)
    // orders (table) --has_column--> orders.amount (column)
    // orders (table) --foreign_key--> customers (table)
    // customers (table) --has_column--> customers.id (column)
    // customers (table) --has_column--> customers.name (column)

    auto add_node = [&](const std::string& id, const std::string& type,
                        const std::string& prop_key,
                        const std::string& prop_val) {
      GraphNode n;
      n.id = id;
      n.node_type = type;
      n.properties[prop_key] = prop_val;
      ASSERT_TRUE(mutation_->AddNode(n).ok());
    };

    add_node("orders", "table", "database", "analytics");
    add_node("customers", "table", "database", "analytics");
    add_node("orders.customer_id", "column", "data_type", "INT64");
    add_node("orders.amount", "column", "data_type", "DOUBLE");
    add_node("customers.id", "column", "data_type", "INT64");
    add_node("customers.name", "column", "data_type", "STRING");

    ASSERT_TRUE(
        mutation_->AddEdge("orders", "orders.customer_id", "has_column", {})
            .ok());
    ASSERT_TRUE(
        mutation_->AddEdge("orders", "orders.amount", "has_column", {}).ok());
    ASSERT_TRUE(
        mutation_
            ->AddEdge("orders", "customers", "foreign_key",
                       {{"on_column", "customer_id"}})
            .ok());
    ASSERT_TRUE(
        mutation_->AddEdge("customers", "customers.id", "has_column", {})
            .ok());
    ASSERT_TRUE(
        mutation_->AddEdge("customers", "customers.name", "has_column", {})
            .ok());
  }

  void TearDown() override {
    traversal_.reset();
    mutation_.reset();
    storage_.reset();
    system(("rm -rf " + test_dir_).c_str());
  }
};

std::string TraversalTest::test_dir_ = "/tmp/zvec_traversal_test";

// 1. Single hop from orders: orders + 3 neighbors, 3 edges
TEST_F(TraversalTest, SingleHopFromOrders) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 1;

  Subgraph sg = traversal_->Traverse(params);

  EXPECT_EQ(sg.nodes.size(), 4u);  // orders + 3 neighbors
  EXPECT_EQ(sg.edges.size(), 3u);
  EXPECT_FALSE(sg.truncated);
}

// 2. Two hops reaches all nodes
TEST_F(TraversalTest, TwoHopsReachesCustomerColumns) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 2;

  Subgraph sg = traversal_->Traverse(params);

  EXPECT_EQ(sg.nodes.size(), 6u);
  EXPECT_EQ(sg.edges.size(), 5u);
}

// 3. Edge filter limits traversal to has_column edges only
TEST_F(TraversalTest, EdgeFilterLimitsTraversal) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 2;
  params.edge_filter = "edge_type = 'has_column'";

  Subgraph sg = traversal_->Traverse(params);

  // Only follows has_column edges from orders: orders.customer_id, orders.amount
  // Does NOT follow foreign_key to customers
  EXPECT_EQ(sg.nodes.size(), 3u);  // orders + 2 columns
  EXPECT_EQ(sg.edges.size(), 2u);
}

// 4. Node filter limits results
TEST_F(TraversalTest, NodeFilterLimitsResults) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 1;
  params.node_filter = "node_type = 'column'";

  Subgraph sg = traversal_->Traverse(params);

  // Seed "orders" always included even though it's a table
  // Only column neighbors pass the filter
  EXPECT_EQ(sg.NodesOfType("column").size(), 2u);
  // orders (seed) is still present
  EXPECT_EQ(sg.NodesOfType("table").size(), 1u);
  // Total: orders + 2 columns
  EXPECT_EQ(sg.nodes.size(), 3u);
}

// 5. Max nodes budget truncates
TEST_F(TraversalTest, MaxNodesBudgetTruncates) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 2;
  params.max_nodes = 3;

  Subgraph sg = traversal_->Traverse(params);

  EXPECT_LE(static_cast<int>(sg.nodes.size()), 3);
  EXPECT_TRUE(sg.truncated);
}

// 6. Cycle detection: customers -> orders cycle
TEST_F(TraversalTest, CycleDetection) {
  // Add a cycle: customers -> orders (learned, undirected)
  ASSERT_TRUE(
      mutation_->AddEdge("customers", "orders", "learned", {}).ok());

  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 10;

  Subgraph sg = traversal_->Traverse(params);

  // Should terminate with all 6 nodes, no infinite loop
  EXPECT_EQ(sg.nodes.size(), 6u);
}

// 7. Empty start IDs
TEST_F(TraversalTest, EmptyStartIds) {
  TraversalParams params;
  params.max_depth = 3;

  Subgraph sg = traversal_->Traverse(params);

  EXPECT_EQ(sg.nodes.size(), 0u);
  EXPECT_EQ(sg.edges.size(), 0u);
}

// 8. Depth zero returns only seeds
TEST_F(TraversalTest, DepthZeroReturnsOnlySeeds) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 0;

  Subgraph sg = traversal_->Traverse(params);

  EXPECT_EQ(sg.nodes.size(), 1u);
  EXPECT_EQ(sg.edges.size(), 0u);
  EXPECT_EQ(sg.nodes[0].id, "orders");
}

// 9. Multiple seeds with deduplication
TEST_F(TraversalTest, MultipleSeeds) {
  TraversalParams params;
  params.start_ids = {"orders", "customers"};
  params.max_depth = 1;

  Subgraph sg = traversal_->Traverse(params);

  EXPECT_EQ(sg.nodes.size(), 6u);  // all nodes, deduplicated
}

// 10. Nonexistent seed is skipped gracefully
TEST_F(TraversalTest, NonexistentSeedSkipped) {
  TraversalParams params;
  params.start_ids = {"nonexistent", "orders"};
  params.max_depth = 0;

  Subgraph sg = traversal_->Traverse(params);

  // Only "orders" should be returned
  EXPECT_EQ(sg.nodes.size(), 1u);
  EXPECT_EQ(sg.nodes[0].id, "orders");
}

// 11. Edge filter that matches nothing: only seed nodes
TEST_F(TraversalTest, EdgeFilterNoMatch) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 2;
  params.edge_filter = "edge_type = 'nonexistent_type'";

  Subgraph sg = traversal_->Traverse(params);

  EXPECT_EQ(sg.nodes.size(), 1u);  // only the seed
  EXPECT_EQ(sg.edges.size(), 0u);
}
