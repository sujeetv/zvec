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

#include "graph/subgraph.h"

#include <sstream>
#include <unordered_map>
#include <unordered_set>

namespace zvec {
namespace graph {

namespace {

//! Escape a string for safe embedding in JSON.
std::string EscapeJson(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 2);
  for (char c : s) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out += c;
        break;
    }
  }
  return out;
}

//! Serialize a string-to-string map as a JSON object.
std::string PropertiesToJson(
    const std::unordered_map<std::string, std::string>& props) {
  std::string out = "{";
  bool first = true;
  for (const auto& [k, v] : props) {
    if (!first) out += ",";
    first = false;
    out += "\"" + EscapeJson(k) + "\":\"" + EscapeJson(v) + "\"";
  }
  out += "}";
  return out;
}

}  // namespace

std::vector<const GraphNode*> Subgraph::NodesOfType(
    const std::string& type) const {
  std::vector<const GraphNode*> result;
  for (const auto& node : nodes) {
    if (node.node_type == type) {
      result.push_back(&node);
    }
  }
  return result;
}

std::vector<const GraphEdge*> Subgraph::EdgesOfType(
    const std::string& type) const {
  std::vector<const GraphEdge*> result;
  for (const auto& edge : edges) {
    if (edge.edge_type == type) {
      result.push_back(&edge);
    }
  }
  return result;
}

std::vector<const GraphEdge*> Subgraph::EdgesFrom(
    const std::string& node_id) const {
  std::vector<const GraphEdge*> result;
  for (const auto& edge : edges) {
    if (edge.source_id == node_id) {
      result.push_back(&edge);
    }
  }
  return result;
}

std::vector<const GraphEdge*> Subgraph::EdgesTo(
    const std::string& node_id) const {
  std::vector<const GraphEdge*> result;
  for (const auto& edge : edges) {
    if (edge.target_id == node_id) {
      result.push_back(&edge);
    }
  }
  return result;
}

std::vector<const GraphNode*> Subgraph::Neighbors(
    const std::string& node_id) const {
  // Collect neighbor ids from edges in both directions.
  std::unordered_set<std::string> neighbor_ids;
  for (const auto& edge : edges) {
    if (edge.source_id == node_id) {
      neighbor_ids.insert(edge.target_id);
    }
    if (edge.target_id == node_id) {
      neighbor_ids.insert(edge.source_id);
    }
  }

  // Build an id -> node pointer map for quick lookup.
  std::unordered_map<std::string, const GraphNode*> node_map;
  for (const auto& node : nodes) {
    node_map[node.id] = &node;
  }

  std::vector<const GraphNode*> result;
  for (const auto& nid : neighbor_ids) {
    auto it = node_map.find(nid);
    if (it != node_map.end()) {
      result.push_back(it->second);
    }
  }
  return result;
}

std::string Subgraph::ToJson() const {
  std::string out = "{\"nodes\":[";
  for (size_t i = 0; i < nodes.size(); ++i) {
    if (i > 0) out += ",";
    const auto& n = nodes[i];
    out += "{\"id\":\"" + EscapeJson(n.id) + "\"";
    out += ",\"node_type\":\"" + EscapeJson(n.node_type) + "\"";
    out += ",\"properties\":" + PropertiesToJson(n.properties);
    out += "}";
  }
  out += "],\"edges\":[";
  for (size_t i = 0; i < edges.size(); ++i) {
    if (i > 0) out += ",";
    const auto& e = edges[i];
    out += "{\"id\":\"" + EscapeJson(e.id) + "\"";
    out += ",\"source_id\":\"" + EscapeJson(e.source_id) + "\"";
    out += ",\"target_id\":\"" + EscapeJson(e.target_id) + "\"";
    out += ",\"edge_type\":\"" + EscapeJson(e.edge_type) + "\"";
    out += ",\"directed\":";
    out += e.directed ? "true" : "false";
    out += ",\"properties\":" + PropertiesToJson(e.properties);
    out += "}";
  }
  out += "],\"truncated\":";
  out += truncated ? "true" : "false";
  out += "}";
  return out;
}

std::string Subgraph::ToText() const {
  std::ostringstream os;
  os << "Subgraph: " << nodes.size() << " nodes, " << edges.size()
     << " edges\n";

  os << "Nodes:\n";
  for (const auto& n : nodes) {
    os << "  [" << n.node_type << "] " << n.id;
    if (!n.properties.empty()) {
      os << " {";
      bool first = true;
      for (const auto& [k, v] : n.properties) {
        if (!first) os << ", ";
        first = false;
        os << k << ": " << v;
      }
      os << "}";
    }
    os << "\n";
  }

  os << "Edges:\n";
  for (const auto& e : edges) {
    os << "  " << e.source_id << " --" << e.edge_type
       << (e.directed ? "--> " : "-- ") << e.target_id << "\n";
  }

  return os.str();
}

}  // namespace graph
}  // namespace zvec
