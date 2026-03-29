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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "graph/proto/graph.pb.h"
#include <zvec/db/status.h>

namespace zvec {
namespace graph {

//! Builder for NodeTypeDef protobuf message
class NodeTypeBuilder {
 public:
  explicit NodeTypeBuilder(const std::string& name);

  //! Add a scalar property to the node type
  NodeTypeBuilder& AddProperty(const std::string& name,
                               zvec::proto::DataType data_type,
                               bool nullable);

  //! Add a vector field to the node type
  NodeTypeBuilder& AddVector(const std::string& name,
                             zvec::proto::DataType data_type,
                             uint32_t dimension);

  //! Build and return the NodeTypeDef protobuf message
  zvec::graph::proto::NodeTypeDef Build() const;

 private:
  zvec::graph::proto::NodeTypeDef def_;
};

//! Builder for EdgeTypeDef protobuf message
class EdgeTypeBuilder {
 public:
  explicit EdgeTypeBuilder(const std::string& name, bool directed);

  //! Add a scalar property to the edge type
  EdgeTypeBuilder& AddProperty(const std::string& name,
                               zvec::proto::DataType data_type,
                               bool nullable);

  //! Add a vector field to the edge type
  EdgeTypeBuilder& AddVector(const std::string& name,
                             zvec::proto::DataType data_type,
                             uint32_t dimension);

  //! Build and return the EdgeTypeDef protobuf message
  zvec::graph::proto::EdgeTypeDef Build() const;

 private:
  zvec::graph::proto::EdgeTypeDef def_;
};

//! In-memory graph schema — validates nodes/edges, serializes to protobuf
class GraphSchema {
 public:
  explicit GraphSchema(const std::string& name);

  //! Get the schema name
  const std::string& Name() const;

  //! Get the number of registered node types
  size_t NodeTypeCount() const;

  //! Get the number of registered edge types
  size_t EdgeTypeCount() const;

  //! Register a new node type; returns error if name already exists
  Status AddNodeType(const zvec::graph::proto::NodeTypeDef& node_type);

  //! Register a new edge type; returns error if name already exists
  Status AddEdgeType(const zvec::graph::proto::EdgeTypeDef& edge_type);

  //! Add a constraint specifying which node types an edge type can connect
  Status AddEdgeConstraint(const std::string& edge_type,
                           const std::string& source_node_type,
                           const std::string& target_node_type);

  //! Look up a node type by name; returns nullptr if not found
  const zvec::graph::proto::NodeTypeDef* GetNodeType(
      const std::string& name) const;

  //! Look up an edge type by name; returns nullptr if not found
  const zvec::graph::proto::EdgeTypeDef* GetEdgeType(
      const std::string& name) const;

  //! Validate a node against the schema (type exists, required props present)
  Status ValidateNode(
      const std::string& node_type,
      const std::unordered_map<std::string, std::string>& properties) const;

  //! Validate an edge against the schema (type exists, constraints satisfied)
  Status ValidateEdge(const std::string& edge_type,
                      const std::string& source_node_type,
                      const std::string& target_node_type,
                      const std::string& source_id,
                      const std::string& target_id) const;

  //! Serialize this schema to a protobuf message
  zvec::graph::proto::GraphSchemaDef ToProto() const;

  //! Deserialize a schema from a protobuf message
  static GraphSchema FromProto(
      const zvec::graph::proto::GraphSchemaDef& proto);

 private:
  std::string name_;
  std::unordered_map<std::string, zvec::graph::proto::NodeTypeDef>
      node_types_;
  std::unordered_map<std::string, zvec::graph::proto::EdgeTypeDef>
      edge_types_;
  // edge_type -> vector of (source_node_type, target_node_type)
  std::unordered_map<std::string,
                     std::vector<std::pair<std::string, std::string>>>
      edge_constraints_;
};

}  // namespace graph
}  // namespace zvec
