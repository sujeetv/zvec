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

//! A graph collection backed by a RocksDB KV store.
//!
//! Replaces GraphEngine with a storage-optimized implementation for
//! concurrent agent access. All data lives in RocksDB column families;
//! vector embeddings are stored separately in zvec Collections.
class GraphCollection {
 public:
  //! Create a new graph collection at the given path.
  static std::unique_ptr<GraphCollection> Create(const std::string& path,
                                                  const GraphSchema& schema);

  //! Open an existing graph collection from disk.
  static std::unique_ptr<GraphCollection> Open(const std::string& path);

  //! Destroy the collection and all data.
  void Destroy();

  //! Flush pending writes to disk.
  Status Flush();

  //! Get the graph schema.
  const GraphSchema& GetSchema() const;

  // --- Node operations ---

  Status AddNode(const GraphNode& node);
  Status RemoveNode(const std::string& node_id);
  Status UpdateNode(
      const std::string& node_id,
      const std::unordered_map<std::string, std::string>& properties);
  std::vector<GraphNode> FetchNodes(const std::vector<std::string>& ids);
  std::vector<GraphNode> FilterNodes(const std::string& filter_expr,
                                     int limit = 1000);

  // --- Edge operations ---

  Status AddEdge(
      const std::string& source_id, const std::string& target_id,
      const std::string& edge_type,
      const std::unordered_map<std::string, std::string>& properties);
  Status RemoveEdge(const std::string& edge_id);
  Status UpdateEdge(
      const std::string& edge_id,
      const std::unordered_map<std::string, std::string>& properties);
  std::vector<GraphEdge> FetchEdges(const std::vector<std::string>& ids);
  std::vector<GraphEdge> FilterEdges(const std::string& filter_expr,
                                     int limit = 1000);

  // --- Traversal ---

  Subgraph Traverse(const TraversalParams& params);

  // --- Index operations ---

  Status CreateIndex(const std::string& entity, const std::string& field);
  Status DropIndex(const std::string& entity, const std::string& field);

 private:
  GraphCollection() = default;

  std::string path_;
  std::unique_ptr<GraphSchema> schema_;
  std::unique_ptr<StorageInterface> storage_;
  std::unique_ptr<MutationEngine> mutation_;
  std::unique_ptr<TraversalEngine> traversal_;
};

}  // namespace graph
}  // namespace zvec
