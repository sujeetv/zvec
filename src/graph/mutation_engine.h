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

#include <mutex>
#include <string>
#include <unordered_map>

#include "graph/graph_edge.h"
#include "graph/graph_node.h"
#include "graph/graph_schema.h"
#include "graph/storage/storage_interface.h"

namespace zvec {
namespace graph {

//! Validates mutations against the schema, manages bidirectional adjacency
//! lists, and provides atomic edge writes via StorageInterface::AtomicBatch.
class MutationEngine {
 public:
  MutationEngine(const GraphSchema* schema, StorageInterface* storage);

  //! Add a node (validates against schema, sets version/timestamp)
  Status AddNode(const GraphNode& node);

  //! Remove a node and cascade-delete all connected edges
  Status RemoveNode(const std::string& node_id);

  //! Update specific properties on an existing node
  Status UpdateNode(
      const std::string& node_id,
      const std::unordered_map<std::string, std::string>& properties);

  //! Add an edge (atomic: edge doc + both adjacency lists)
  Status AddEdge(
      const std::string& source_id, const std::string& target_id,
      const std::string& edge_type,
      const std::unordered_map<std::string, std::string>& properties);

  //! Remove an edge and clean up adjacency lists
  Status RemoveEdge(const std::string& edge_id);

  //! Update specific properties on an existing edge
  Status UpdateEdge(
      const std::string& edge_id,
      const std::unordered_map<std::string, std::string>& properties);

 private:
  const GraphSchema* schema_;
  StorageInterface* storage_;
  std::mutex mutex_;

  //! Generate a deterministic edge ID: "source_id:edge_type:target_id"
  static std::string MakeEdgeId(const std::string& source,
                                const std::string& edge_type,
                                const std::string& target);

  //! Return current time as epoch milliseconds
  static uint64_t Now();
};

}  // namespace graph
}  // namespace zvec
