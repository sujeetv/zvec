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

#include "graph/graph_schema.h"

namespace zvec {
namespace graph {

// --- NodeTypeBuilder ---

NodeTypeBuilder::NodeTypeBuilder(const std::string& name) {
  def_.set_name(name);
}

NodeTypeBuilder& NodeTypeBuilder::AddProperty(
    const std::string& name, zvec::proto::DataType data_type, bool nullable) {
  auto* prop = def_.add_properties();
  prop->set_name(name);
  prop->set_data_type(data_type);
  prop->set_nullable(nullable);
  return *this;
}

NodeTypeBuilder& NodeTypeBuilder::AddVector(const std::string& name,
                                            zvec::proto::DataType data_type,
                                            uint32_t dimension) {
  auto* vec = def_.add_vectors();
  vec->set_name(name);
  vec->set_data_type(data_type);
  vec->set_dimension(dimension);
  return *this;
}

zvec::graph::proto::NodeTypeDef NodeTypeBuilder::Build() const {
  return def_;
}

// --- EdgeTypeBuilder ---

EdgeTypeBuilder::EdgeTypeBuilder(const std::string& name, bool directed) {
  def_.set_name(name);
  def_.set_directed(directed);
}

EdgeTypeBuilder& EdgeTypeBuilder::AddProperty(
    const std::string& name, zvec::proto::DataType data_type, bool nullable) {
  auto* prop = def_.add_properties();
  prop->set_name(name);
  prop->set_data_type(data_type);
  prop->set_nullable(nullable);
  return *this;
}

EdgeTypeBuilder& EdgeTypeBuilder::AddVector(const std::string& name,
                                            zvec::proto::DataType data_type,
                                            uint32_t dimension) {
  auto* vec = def_.add_vectors();
  vec->set_name(name);
  vec->set_data_type(data_type);
  vec->set_dimension(dimension);
  return *this;
}

zvec::graph::proto::EdgeTypeDef EdgeTypeBuilder::Build() const {
  return def_;
}

// --- GraphSchema ---

GraphSchema::GraphSchema(const std::string& name) : name_(name) {}

const std::string& GraphSchema::Name() const {
  return name_;
}

size_t GraphSchema::NodeTypeCount() const {
  return node_types_.size();
}

size_t GraphSchema::EdgeTypeCount() const {
  return edge_types_.size();
}

Status GraphSchema::AddNodeType(
    const zvec::graph::proto::NodeTypeDef& node_type) {
  const auto& name = node_type.name();
  if (node_types_.count(name) > 0) {
    return Status::AlreadyExists("Node type '", name, "' already exists");
  }
  node_types_.emplace(name, node_type);
  return Status::OK();
}

Status GraphSchema::AddEdgeType(
    const zvec::graph::proto::EdgeTypeDef& edge_type) {
  const auto& name = edge_type.name();
  if (edge_types_.count(name) > 0) {
    return Status::AlreadyExists("Edge type '", name, "' already exists");
  }
  edge_types_.emplace(name, edge_type);
  return Status::OK();
}

Status GraphSchema::AddEdgeConstraint(const std::string& edge_type,
                                      const std::string& source_node_type,
                                      const std::string& target_node_type) {
  if (edge_types_.count(edge_type) == 0) {
    return Status::NotFound("Edge type '", edge_type, "' not found");
  }
  if (node_types_.count(source_node_type) == 0) {
    return Status::NotFound("Source node type '", source_node_type,
                            "' not found");
  }
  if (node_types_.count(target_node_type) == 0) {
    return Status::NotFound("Target node type '", target_node_type,
                            "' not found");
  }
  edge_constraints_[edge_type].emplace_back(source_node_type,
                                            target_node_type);
  return Status::OK();
}

const zvec::graph::proto::NodeTypeDef* GraphSchema::GetNodeType(
    const std::string& name) const {
  auto it = node_types_.find(name);
  if (it == node_types_.end()) {
    return nullptr;
  }
  return &it->second;
}

const zvec::graph::proto::EdgeTypeDef* GraphSchema::GetEdgeType(
    const std::string& name) const {
  auto it = edge_types_.find(name);
  if (it == edge_types_.end()) {
    return nullptr;
  }
  return &it->second;
}

Status GraphSchema::ValidateNode(
    const std::string& node_type,
    const std::unordered_map<std::string, std::string>& properties) const {
  auto it = node_types_.find(node_type);
  if (it == node_types_.end()) {
    return Status::NotFound("Node type '", node_type, "' not found");
  }
  const auto& def = it->second;
  for (int i = 0; i < def.properties_size(); ++i) {
    const auto& prop = def.properties(i);
    if (!prop.nullable() && properties.count(prop.name()) == 0) {
      return Status::InvalidArgument("Required property '", prop.name(),
                                     "' missing for node type '", node_type,
                                     "'");
    }
  }
  return Status::OK();
}

Status GraphSchema::ValidateEdge(const std::string& edge_type,
                                 const std::string& source_node_type,
                                 const std::string& target_node_type,
                                 const std::string& /*source_id*/,
                                 const std::string& /*target_id*/) const {
  if (edge_types_.count(edge_type) == 0) {
    return Status::NotFound("Edge type '", edge_type, "' not found");
  }
  auto cit = edge_constraints_.find(edge_type);
  if (cit != edge_constraints_.end()) {
    const auto& constraints = cit->second;
    bool found = false;
    for (const auto& c : constraints) {
      if (c.first == source_node_type && c.second == target_node_type) {
        found = true;
        break;
      }
    }
    if (!found) {
      return Status::InvalidArgument(
          "Edge type '", edge_type, "' does not allow connection from '",
          source_node_type, "' to '", target_node_type, "'");
    }
  }
  return Status::OK();
}

zvec::graph::proto::GraphSchemaDef GraphSchema::ToProto() const {
  zvec::graph::proto::GraphSchemaDef proto;
  proto.set_name(name_);

  for (const auto& [name, def] : node_types_) {
    *proto.add_node_types() = def;
  }
  for (const auto& [name, def] : edge_types_) {
    *proto.add_edge_types() = def;
  }
  for (const auto& [edge_type, pairs] : edge_constraints_) {
    for (const auto& [src, tgt] : pairs) {
      auto* constraint = proto.add_edge_constraints();
      constraint->set_edge_type(edge_type);
      constraint->set_source_node_type(src);
      constraint->set_target_node_type(tgt);
    }
  }
  return proto;
}

GraphSchema GraphSchema::FromProto(
    const zvec::graph::proto::GraphSchemaDef& proto) {
  GraphSchema schema(proto.name());
  for (int i = 0; i < proto.node_types_size(); ++i) {
    schema.AddNodeType(proto.node_types(i));
  }
  for (int i = 0; i < proto.edge_types_size(); ++i) {
    schema.AddEdgeType(proto.edge_types(i));
  }
  for (int i = 0; i < proto.edge_constraints_size(); ++i) {
    const auto& c = proto.edge_constraints(i);
    schema.AddEdgeConstraint(c.edge_type(), c.source_node_type(),
                             c.target_node_type());
  }
  return schema;
}

}  // namespace graph
}  // namespace zvec
