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

#include <string>
#include <vector>

#include "graph/graph_edge.h"
#include "graph/graph_node.h"

namespace zvec {
namespace graph {

struct Subgraph {
  std::vector<GraphNode> nodes;
  std::vector<GraphEdge> edges;
  bool truncated = false;

  //! Return pointers to all nodes matching the given type.
  std::vector<const GraphNode*> NodesOfType(const std::string& type) const;

  //! Return pointers to all edges matching the given type.
  std::vector<const GraphEdge*> EdgesOfType(const std::string& type) const;

  //! Return pointers to all edges originating from the given node.
  std::vector<const GraphEdge*> EdgesFrom(const std::string& node_id) const;

  //! Return pointers to all edges targeting the given node.
  std::vector<const GraphEdge*> EdgesTo(const std::string& node_id) const;

  //! Return pointers to all neighbor nodes of the given node (via edges).
  std::vector<const GraphNode*> Neighbors(const std::string& node_id) const;

  //! Serialize the subgraph to a JSON string.
  std::string ToJson() const;

  //! Serialize the subgraph to an agent-readable text summary.
  std::string ToText() const;
};

}  // namespace graph
}  // namespace zvec
