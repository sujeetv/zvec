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

#include "graph/graph_edge.h"
#include "graph/graph_node.h"
#include "graph/graph_schema.h"
#include "graph/mutation_engine.h"
#include "graph/storage/storage_interface.h"
#include "graph/subgraph.h"
#include "graph/traversal.h"

namespace zvec {
namespace graph {

//! Unified entry point for the property graph engine.
//!
//! Owns schema, storage, mutation engine, and traversal engine.
//! Provides create/open/destroy lifecycle plus all graph operations.
class GraphEngine {
 public:
  //! Create a new graph at the given path with the specified schema.
  static std::unique_ptr<GraphEngine> Create(const std::string& path,
                                             const GraphSchema& schema);

  //! Open an existing graph from disk.
  static std::unique_ptr<GraphEngine> Open(const std::string& path);

  //! Destroy the graph (delete all storage and metadata).
  void Destroy();

  //! Repair orphaned adjacency references (stub for future enhancement).
  Status Repair();

  //! Get the graph schema.
  const GraphSchema& GetSchema() const;

  // --- Node operations ---

  //! Add a node to the graph (validates against schema).
  Status AddNode(const GraphNode& node);

  //! Remove a node and cascade-delete all connected edges.
  Status RemoveNode(const std::string& node_id);

  //! Update specific properties on an existing node.
  Status UpdateNode(
      const std::string& node_id,
      const std::unordered_map<std::string, std::string>& properties);

  //! Fetch nodes by their IDs.
  std::vector<GraphNode> FetchNodes(const std::vector<std::string>& ids);

  // --- Edge operations ---

  //! Add an edge between two existing nodes (validates against schema).
  Status AddEdge(
      const std::string& source_id, const std::string& target_id,
      const std::string& edge_type,
      const std::unordered_map<std::string, std::string>& properties);

  //! Remove an edge and clean up adjacency lists.
  Status RemoveEdge(const std::string& edge_id);

  //! Fetch edges by their IDs.
  std::vector<GraphEdge> FetchEdges(const std::vector<std::string>& ids);

  // --- Traversal ---

  //! Perform a multi-hop BFS traversal.
  Subgraph Traverse(const TraversalParams& params);

 private:
  GraphEngine() = default;

  std::string path_;
  std::unique_ptr<GraphSchema> schema_;
  std::unique_ptr<StorageInterface> storage_;
  std::unique_ptr<MutationEngine> mutation_;
  std::unique_ptr<TraversalEngine> traversal_;
};

}  // namespace graph
}  // namespace zvec
