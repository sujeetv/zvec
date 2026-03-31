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

#include "graph/storage/storage_interface.h"
#include "graph/subgraph.h"

namespace zvec {
namespace graph {

//! Parameters controlling a multi-hop BFS traversal.
struct TraversalParams {
  std::vector<std::string> start_ids;
  int max_depth = 3;
  int max_nodes = 0;        // 0 = unlimited
  int beam_width = 0;       // 0 = unlimited
  std::string edge_filter;  // e.g. "edge_type = 'has_column'"
  std::string node_filter;  // e.g. "node_type = 'column'"
};

//! BFS traversal engine with edge/node filtering, budget control, and cycle
//! detection.  Core of the GraphRAG query path.
class TraversalEngine {
 public:
  explicit TraversalEngine(StorageInterface* storage);

  //! Perform a multi-hop BFS traversal starting from the given seed nodes.
  Subgraph Traverse(const TraversalParams& params) const;

 private:
  StorageInterface* storage_;

  //! Parse a simple "field = 'value'" filter string.
  //! Returns true if parsing succeeded, false otherwise.
  static bool ParseFilter(const std::string& filter, std::string& field,
                          std::string& value);

  //! Check whether an edge matches the given filter field/value.
  static bool EdgeMatchesFilter(const GraphEdge& edge,
                                const std::string& field,
                                const std::string& value);

  //! Check whether a node matches the given filter field/value.
  static bool NodeMatchesFilter(const GraphNode& node,
                                const std::string& field,
                                const std::string& value);
};

}  // namespace graph
}  // namespace zvec
