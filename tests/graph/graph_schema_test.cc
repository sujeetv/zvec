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

#include "graph/graph_schema.h"

class GraphSchemaTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(GraphSchemaTest, ConstructEmptySchema) {
  zvec::graph::GraphSchema schema("test_graph");
  EXPECT_EQ(schema.Name(), "test_graph");
  EXPECT_EQ(schema.NodeTypeCount(), 0);
  EXPECT_EQ(schema.EdgeTypeCount(), 0);
}

TEST_F(GraphSchemaTest, AddNodeType) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::NodeTypeBuilder builder("table");
  builder.AddProperty("database", zvec::proto::DT_STRING, false);
  builder.AddProperty("row_count", zvec::proto::DT_INT64, true);
  builder.AddVector("desc_emb", zvec::proto::DT_VECTOR_FP32, 768);

  auto status = schema.AddNodeType(builder.Build());
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(schema.NodeTypeCount(), 1);

  auto node_type = schema.GetNodeType("table");
  ASSERT_NE(node_type, nullptr);
  EXPECT_EQ(node_type->name(), "table");
  EXPECT_EQ(node_type->properties_size(), 2);
  EXPECT_EQ(node_type->vectors_size(), 1);
}

TEST_F(GraphSchemaTest, AddEdgeType) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::EdgeTypeBuilder builder("foreign_key", /*directed=*/true);
  builder.AddProperty("on_column", zvec::proto::DT_STRING, false);

  auto status = schema.AddEdgeType(builder.Build());
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(schema.EdgeTypeCount(), 1);
}

TEST_F(GraphSchemaTest, DuplicateNodeTypeRejected) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::NodeTypeBuilder builder("table");
  EXPECT_TRUE(schema.AddNodeType(builder.Build()).ok());
  EXPECT_FALSE(schema.AddNodeType(builder.Build()).ok());
}

TEST_F(GraphSchemaTest, AddEdgeConstraint) {
  zvec::graph::GraphSchema schema("test_graph");

  schema.AddNodeType(zvec::graph::NodeTypeBuilder("table").Build());
  schema.AddNodeType(zvec::graph::NodeTypeBuilder("column").Build());
  schema.AddEdgeType(
      zvec::graph::EdgeTypeBuilder("has_column", true).Build());

  auto status = schema.AddEdgeConstraint("has_column", "table", "column");
  EXPECT_TRUE(status.ok());
}

TEST_F(GraphSchemaTest, EdgeConstraintInvalidNodeTypeRejected) {
  zvec::graph::GraphSchema schema("test_graph");
  schema.AddEdgeType(
      zvec::graph::EdgeTypeBuilder("has_column", true).Build());

  // "table" node type doesn't exist
  auto status = schema.AddEdgeConstraint("has_column", "table", "column");
  EXPECT_FALSE(status.ok());
}

TEST_F(GraphSchemaTest, ValidateNodeAcceptsConformingNode) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::NodeTypeBuilder builder("table");
  builder.AddProperty("database", zvec::proto::DT_STRING, false);
  schema.AddNodeType(builder.Build());

  std::unordered_map<std::string, std::string> props = {
      {"database", "analytics"}};
  auto status = schema.ValidateNode("table", props);
  EXPECT_TRUE(status.ok());
}

TEST_F(GraphSchemaTest, ValidateNodeRejectsUnknownType) {
  zvec::graph::GraphSchema schema("test_graph");
  std::unordered_map<std::string, std::string> props;
  auto status = schema.ValidateNode("unknown_type", props);
  EXPECT_FALSE(status.ok());
}

TEST_F(GraphSchemaTest, ValidateEdgeRejectsConstraintViolation) {
  zvec::graph::GraphSchema schema("test_graph");
  schema.AddNodeType(zvec::graph::NodeTypeBuilder("table").Build());
  schema.AddNodeType(zvec::graph::NodeTypeBuilder("column").Build());

  zvec::graph::EdgeTypeBuilder eb("has_column", true);
  schema.AddEdgeType(eb.Build());
  schema.AddEdgeConstraint("has_column", "table", "column");

  // column -> table violates the constraint (should be table -> column)
  auto status = schema.ValidateEdge("has_column", "column", "table",
                                    "col_node", "tbl_node");
  EXPECT_FALSE(status.ok());
}

TEST_F(GraphSchemaTest, SerializeAndDeserializeRoundTrip) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::NodeTypeBuilder nb("table");
  nb.AddProperty("database", zvec::proto::DT_STRING, false);
  nb.AddVector("desc_emb", zvec::proto::DT_VECTOR_FP32, 768);
  schema.AddNodeType(nb.Build());

  zvec::graph::EdgeTypeBuilder eb("foreign_key", true);
  eb.AddProperty("on_column", zvec::proto::DT_STRING, false);
  schema.AddEdgeType(eb.Build());

  // Serialize
  auto proto = schema.ToProto();
  EXPECT_EQ(proto.name(), "test_graph");

  // Deserialize
  auto restored = zvec::graph::GraphSchema::FromProto(proto);
  EXPECT_EQ(restored.Name(), "test_graph");
  EXPECT_EQ(restored.NodeTypeCount(), 1);
  EXPECT_EQ(restored.EdgeTypeCount(), 1);

  auto nt = restored.GetNodeType("table");
  ASSERT_NE(nt, nullptr);
  EXPECT_EQ(nt->properties_size(), 1);
  EXPECT_EQ(nt->vectors_size(), 1);
}
