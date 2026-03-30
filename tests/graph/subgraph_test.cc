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

#include "graph/subgraph.h"

using namespace zvec::graph;

class SubgraphTest : public testing::Test {
 protected:
  Subgraph MakeSampleSubgraph() {
    Subgraph sg;

    GraphNode n1;
    n1.id = "orders";
    n1.node_type = "table";
    n1.properties["database"] = "analytics";
    n1.properties["row_count"] = "1000000";

    GraphNode n2;
    n2.id = "orders.customer_id";
    n2.node_type = "column";
    n2.properties["data_type"] = "INT64";

    GraphEdge e1;
    e1.id = "orders:has_column:orders.customer_id";
    e1.source_id = "orders";
    e1.target_id = "orders.customer_id";
    e1.edge_type = "has_column";
    e1.directed = true;

    sg.nodes.push_back(std::move(n1));
    sg.nodes.push_back(std::move(n2));
    sg.edges.push_back(std::move(e1));
    return sg;
  }
};

TEST_F(SubgraphTest, NodesOfType) {
  auto sg = MakeSampleSubgraph();
  auto tables = sg.NodesOfType("table");
  EXPECT_EQ(tables.size(), 1);
  EXPECT_EQ(tables[0]->id, "orders");

  auto columns = sg.NodesOfType("column");
  EXPECT_EQ(columns.size(), 1);
}

TEST_F(SubgraphTest, EdgesOfType) {
  auto sg = MakeSampleSubgraph();
  auto edges = sg.EdgesOfType("has_column");
  EXPECT_EQ(edges.size(), 1);
  EXPECT_EQ(edges[0]->source_id, "orders");
}

TEST_F(SubgraphTest, EdgesFrom) {
  auto sg = MakeSampleSubgraph();
  auto edges = sg.EdgesFrom("orders");
  EXPECT_EQ(edges.size(), 1);
}

TEST_F(SubgraphTest, EdgesTo) {
  auto sg = MakeSampleSubgraph();
  auto edges = sg.EdgesTo("orders.customer_id");
  EXPECT_EQ(edges.size(), 1);
}

TEST_F(SubgraphTest, Neighbors) {
  auto sg = MakeSampleSubgraph();
  auto nbrs = sg.Neighbors("orders");
  EXPECT_EQ(nbrs.size(), 1);
  EXPECT_EQ(nbrs[0]->id, "orders.customer_id");
}

TEST_F(SubgraphTest, ToJsonContainsNodesAndEdges) {
  auto sg = MakeSampleSubgraph();
  auto json = sg.ToJson();
  EXPECT_NE(json.find("\"orders\""), std::string::npos);
  EXPECT_NE(json.find("\"has_column\""), std::string::npos);
  EXPECT_NE(json.find("\"nodes\""), std::string::npos);
  EXPECT_NE(json.find("\"edges\""), std::string::npos);
}

TEST_F(SubgraphTest, ToTextContainsReadableSummary) {
  auto sg = MakeSampleSubgraph();
  auto text = sg.ToText();
  EXPECT_NE(text.find("2 nodes, 1 edges"), std::string::npos);
  EXPECT_NE(text.find("[table] orders"), std::string::npos);
  EXPECT_NE(text.find("orders --has_column-->"), std::string::npos);
}

TEST_F(SubgraphTest, EmptySubgraph) {
  Subgraph sg;
  EXPECT_EQ(sg.NodesOfType("anything").size(), 0);
  EXPECT_EQ(sg.EdgesOfType("anything").size(), 0);
  auto json = sg.ToJson();
  EXPECT_NE(json.find("\"nodes\""), std::string::npos);
}

TEST_F(SubgraphTest, TruncatedFlag) {
  Subgraph sg;
  sg.truncated = true;
  auto json = sg.ToJson();
  EXPECT_NE(json.find("\"truncated\":true"), std::string::npos);
}
