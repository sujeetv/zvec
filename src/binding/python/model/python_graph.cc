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

#include "python_graph.h"

#include <pybind11/stl.h>

#include "graph/graph_edge.h"
#include "graph/graph_collection.h"
#include "graph/graph_node.h"
#include "graph/graph_schema.h"
#include "graph/subgraph.h"
#include "graph/traversal.h"

namespace zvec {

using namespace zvec::graph;

static void throw_graph_error(const Status& status) {
  if (status.ok()) return;
  switch (status.code()) {
    case StatusCode::NOT_FOUND:
      throw py::key_error(status.message());
    case StatusCode::INVALID_ARGUMENT:
      throw py::value_error(status.message());
    default:
      throw std::runtime_error(status.message());
  }
}

void ZVecPyGraph::Initialize(py::module_& m) {
  // --- NodeTypeBuilder ---
  py::class_<NodeTypeBuilder>(m, "_NodeTypeBuilder")
      .def(py::init<const std::string&>(), py::arg("name"))
      .def(
          "add_property",
          [](NodeTypeBuilder& self, const std::string& name, int data_type,
             bool nullable) -> NodeTypeBuilder& {
            return self.AddProperty(
                name, static_cast<zvec::proto::DataType>(data_type), nullable);
          },
          py::arg("name"), py::arg("data_type"), py::arg("nullable") = false)
      .def(
          "add_vector",
          [](NodeTypeBuilder& self, const std::string& name, int data_type,
             uint32_t dimension) -> NodeTypeBuilder& {
            return self.AddVector(
                name, static_cast<zvec::proto::DataType>(data_type), dimension);
          },
          py::arg("name"), py::arg("data_type"), py::arg("dimension"))
      .def("build", &NodeTypeBuilder::Build);

  // --- EdgeTypeBuilder ---
  py::class_<EdgeTypeBuilder>(m, "_EdgeTypeBuilder")
      .def(py::init<const std::string&, bool>(), py::arg("name"),
           py::arg("directed") = true)
      .def(
          "add_property",
          [](EdgeTypeBuilder& self, const std::string& name, int data_type,
             bool nullable) -> EdgeTypeBuilder& {
            return self.AddProperty(
                name, static_cast<zvec::proto::DataType>(data_type), nullable);
          },
          py::arg("name"), py::arg("data_type"), py::arg("nullable") = false)
      .def(
          "add_vector",
          [](EdgeTypeBuilder& self, const std::string& name, int data_type,
             uint32_t dimension) -> EdgeTypeBuilder& {
            return self.AddVector(
                name, static_cast<zvec::proto::DataType>(data_type), dimension);
          },
          py::arg("name"), py::arg("data_type"), py::arg("dimension"))
      .def("build", &EdgeTypeBuilder::Build);

  // --- NodeTypeDef (read-only proto wrapper) ---
  py::class_<zvec::graph::proto::NodeTypeDef>(m, "_NodeTypeDef")
      .def("name", &zvec::graph::proto::NodeTypeDef::name);

  // --- EdgeTypeDef (read-only proto wrapper) ---
  py::class_<zvec::graph::proto::EdgeTypeDef>(m, "_EdgeTypeDef")
      .def("name", &zvec::graph::proto::EdgeTypeDef::name)
      .def("directed", &zvec::graph::proto::EdgeTypeDef::directed);

  // --- GraphSchemaDef (proto wrapper for serialization) ---
  py::class_<zvec::graph::proto::GraphSchemaDef>(m, "_GraphSchemaDef")
      .def(py::init<>())
      .def("SerializeToString",
           [](const zvec::graph::proto::GraphSchemaDef& self) {
             std::string out;
             self.SerializeToString(&out);
             return py::bytes(out);
           })
      .def("ParseFromString",
           [](zvec::graph::proto::GraphSchemaDef& self,
              const std::string& data) { return self.ParseFromString(data); });

  // --- GraphSchema ---
  py::class_<GraphSchema>(m, "_GraphSchema")
      .def(py::init<const std::string&>(), py::arg("name"))
      .def("name", &GraphSchema::Name)
      .def("node_type_count", &GraphSchema::NodeTypeCount)
      .def("edge_type_count", &GraphSchema::EdgeTypeCount)
      .def(
          "add_node_type",
          [](GraphSchema& self,
             const zvec::graph::proto::NodeTypeDef& node_type) {
            auto s = self.AddNodeType(node_type);
            throw_graph_error(s);
          },
          py::arg("node_type"))
      .def(
          "add_edge_type",
          [](GraphSchema& self,
             const zvec::graph::proto::EdgeTypeDef& edge_type) {
            auto s = self.AddEdgeType(edge_type);
            throw_graph_error(s);
          },
          py::arg("edge_type"))
      .def(
          "add_edge_constraint",
          [](GraphSchema& self, const std::string& edge_type,
             const std::string& source, const std::string& target) {
            auto s = self.AddEdgeConstraint(edge_type, source, target);
            throw_graph_error(s);
          },
          py::arg("edge_type"), py::arg("source"), py::arg("target"))
      .def("to_proto", &GraphSchema::ToProto)
      .def_static("from_proto", &GraphSchema::FromProto, py::arg("proto"));

  // --- GraphNode ---
  py::class_<GraphNode>(m, "_GraphNode")
      .def(py::init<>())
      .def_readwrite("id", &GraphNode::id)
      .def_readwrite("node_type", &GraphNode::node_type)
      .def_readwrite("properties", &GraphNode::properties)
      .def_readwrite("vectors", &GraphNode::vectors)
      .def_readwrite("neighbor_ids", &GraphNode::neighbor_ids)
      .def_readwrite("neighbor_edge_ids", &GraphNode::neighbor_edge_ids)
      .def_readwrite("version", &GraphNode::version)
      .def_readwrite("updated_at", &GraphNode::updated_at);

  // --- GraphEdge ---
  py::class_<GraphEdge>(m, "_GraphEdge")
      .def(py::init<>())
      .def_readwrite("id", &GraphEdge::id)
      .def_readwrite("source_id", &GraphEdge::source_id)
      .def_readwrite("target_id", &GraphEdge::target_id)
      .def_readwrite("edge_type", &GraphEdge::edge_type)
      .def_readwrite("directed", &GraphEdge::directed)
      .def_readwrite("properties", &GraphEdge::properties)
      .def_readwrite("vectors", &GraphEdge::vectors)
      .def_readwrite("version", &GraphEdge::version)
      .def_readwrite("updated_at", &GraphEdge::updated_at);

  // --- Subgraph ---
  py::class_<Subgraph>(m, "_Subgraph")
      .def(py::init<>())
      .def_readwrite("nodes", &Subgraph::nodes)
      .def_readwrite("edges", &Subgraph::edges)
      .def_readwrite("truncated", &Subgraph::truncated)
      .def("to_json", &Subgraph::ToJson)
      .def("to_text", &Subgraph::ToText)
      .def(
          "nodes_of_type",
          [](const Subgraph& self, const std::string& type) {
            auto ptrs = self.NodesOfType(type);
            std::vector<GraphNode> result;
            result.reserve(ptrs.size());
            for (auto* p : ptrs) result.push_back(*p);
            return result;
          },
          py::arg("type"))
      .def(
          "edges_of_type",
          [](const Subgraph& self, const std::string& type) {
            auto ptrs = self.EdgesOfType(type);
            std::vector<GraphEdge> result;
            result.reserve(ptrs.size());
            for (auto* p : ptrs) result.push_back(*p);
            return result;
          },
          py::arg("type"))
      .def(
          "edges_from",
          [](const Subgraph& self, const std::string& node_id) {
            auto ptrs = self.EdgesFrom(node_id);
            std::vector<GraphEdge> result;
            result.reserve(ptrs.size());
            for (auto* p : ptrs) result.push_back(*p);
            return result;
          },
          py::arg("node_id"))
      .def(
          "edges_to",
          [](const Subgraph& self, const std::string& node_id) {
            auto ptrs = self.EdgesTo(node_id);
            std::vector<GraphEdge> result;
            result.reserve(ptrs.size());
            for (auto* p : ptrs) result.push_back(*p);
            return result;
          },
          py::arg("node_id"))
      .def(
          "neighbors",
          [](const Subgraph& self, const std::string& node_id) {
            auto ptrs = self.Neighbors(node_id);
            std::vector<GraphNode> result;
            result.reserve(ptrs.size());
            for (auto* p : ptrs) result.push_back(*p);
            return result;
          },
          py::arg("node_id"));

  // --- TraversalParams ---
  py::class_<TraversalParams>(m, "_TraversalParams")
      .def(py::init<>())
      .def_readwrite("start_ids", &TraversalParams::start_ids)
      .def_readwrite("max_depth", &TraversalParams::max_depth)
      .def_readwrite("max_nodes", &TraversalParams::max_nodes)
      .def_readwrite("beam_width", &TraversalParams::beam_width)
      .def_readwrite("edge_filter", &TraversalParams::edge_filter)
      .def_readwrite("node_filter", &TraversalParams::node_filter);

  // --- GraphCollection ---
  py::class_<GraphCollection>(m, "_GraphCollection")
      .def_static(
          "Create",
          [](const std::string& path, const GraphSchema& schema) {
            auto gc = GraphCollection::Create(path, schema);
            if (!gc) {
              throw std::runtime_error(
                  "Failed to create graph at '" + path + "'");
            }
            return gc;
          },
          py::arg("path"), py::arg("schema"),
          py::call_guard<py::gil_scoped_release>())
      .def_static(
          "Open",
          [](const std::string& path) {
            auto gc = GraphCollection::Open(path);
            if (!gc) {
              throw std::runtime_error(
                  "Failed to open graph at '" + path + "'");
            }
            return gc;
          },
          py::arg("path"), py::call_guard<py::gil_scoped_release>())
      .def(
          "Destroy", [](GraphCollection& self) { self.Destroy(); },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "Flush",
          [](GraphCollection& self) {
            auto s = self.Flush();
            throw_graph_error(s);
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "GetSchema",
          [](const GraphCollection& self) -> const GraphSchema& {
            return self.GetSchema();
          },
          py::return_value_policy::reference_internal)
      .def(
          "AddNode",
          [](GraphCollection& self, const GraphNode& node) {
            auto s = self.AddNode(node);
            throw_graph_error(s);
          },
          py::arg("node"), py::call_guard<py::gil_scoped_release>())
      .def(
          "RemoveNode",
          [](GraphCollection& self, const std::string& node_id) {
            auto s = self.RemoveNode(node_id);
            throw_graph_error(s);
          },
          py::arg("node_id"), py::call_guard<py::gil_scoped_release>())
      .def(
          "UpdateNode",
          [](GraphCollection& self, const std::string& node_id,
             const std::unordered_map<std::string, std::string>& properties) {
            auto s = self.UpdateNode(node_id, properties);
            throw_graph_error(s);
          },
          py::arg("node_id"), py::arg("properties"),
          py::call_guard<py::gil_scoped_release>())
      .def("FetchNodes", &GraphCollection::FetchNodes, py::arg("ids"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "FilterNodes",
          [](GraphCollection& self, const std::string& filter_expr,
             int limit) {
            return self.FilterNodes(filter_expr, limit);
          },
          py::arg("filter_expr"), py::arg("limit") = 1000,
          py::call_guard<py::gil_scoped_release>())
      .def(
          "AddEdge",
          [](GraphCollection& self, const std::string& source_id,
             const std::string& target_id, const std::string& edge_type,
             const std::unordered_map<std::string, std::string>& properties) {
            auto s = self.AddEdge(source_id, target_id, edge_type, properties);
            throw_graph_error(s);
          },
          py::arg("source_id"), py::arg("target_id"), py::arg("edge_type"),
          py::arg("properties") =
              std::unordered_map<std::string, std::string>{},
          py::call_guard<py::gil_scoped_release>())
      .def(
          "RemoveEdge",
          [](GraphCollection& self, const std::string& edge_id) {
            auto s = self.RemoveEdge(edge_id);
            throw_graph_error(s);
          },
          py::arg("edge_id"), py::call_guard<py::gil_scoped_release>())
      .def(
          "UpdateEdge",
          [](GraphCollection& self, const std::string& edge_id,
             const std::unordered_map<std::string, std::string>& properties) {
            auto s = self.UpdateEdge(edge_id, properties);
            throw_graph_error(s);
          },
          py::arg("edge_id"), py::arg("properties"),
          py::call_guard<py::gil_scoped_release>())
      .def("FetchEdges", &GraphCollection::FetchEdges, py::arg("ids"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "FilterEdges",
          [](GraphCollection& self, const std::string& filter_expr,
             int limit) {
            return self.FilterEdges(filter_expr, limit);
          },
          py::arg("filter_expr"), py::arg("limit") = 1000,
          py::call_guard<py::gil_scoped_release>())
      .def("Traverse", &GraphCollection::Traverse, py::arg("params"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "CreateIndex",
          [](GraphCollection& self, const std::string& entity,
             const std::string& field) {
            auto s = self.CreateIndex(entity, field);
            throw_graph_error(s);
          },
          py::arg("entity"), py::arg("field"),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "DropIndex",
          [](GraphCollection& self, const std::string& entity,
             const std::string& field) {
            auto s = self.DropIndex(entity, field);
            throw_graph_error(s);
          },
          py::arg("entity"), py::arg("field"),
          py::call_guard<py::gil_scoped_release>());
}

}  // namespace zvec
