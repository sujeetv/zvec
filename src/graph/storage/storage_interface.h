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
#include <vector>

#include "graph/graph_edge.h"
#include "graph/graph_node.h"
#include <zvec/db/status.h>

namespace zvec {
namespace graph {

//! Type of mutation in an atomic batch.
enum class MutationType {
  UPSERT_NODE,
  DELETE_NODE,
  UPSERT_EDGE,
  DELETE_EDGE,
};

//! A single mutation operation for use in AtomicBatch.
struct Mutation {
  MutationType type;
  GraphNode node;
  GraphEdge edge;

  //! Create an upsert-node mutation.
  static Mutation UpsertNode(const GraphNode& n) {
    Mutation m;
    m.type = MutationType::UPSERT_NODE;
    m.node = n;
    return m;
  }

  //! Create a delete-node mutation.
  static Mutation DeleteNode(const GraphNode& n) {
    Mutation m;
    m.type = MutationType::DELETE_NODE;
    m.node = n;
    return m;
  }

  //! Create an upsert-edge mutation.
  static Mutation UpsertEdge(const GraphEdge& e) {
    Mutation m;
    m.type = MutationType::UPSERT_EDGE;
    m.edge = e;
    return m;
  }

  //! Create a delete-edge mutation.
  static Mutation DeleteEdge(const GraphEdge& e) {
    Mutation m;
    m.type = MutationType::DELETE_EDGE;
    m.edge = e;
    return m;
  }
};

//! Abstract interface for graph storage backends.
//!
//! Provides CRUD operations for nodes and edges, filter/query support,
//! and an atomic batch mechanism.
class StorageInterface {
 public:
  using UPointer = std::unique_ptr<StorageInterface>;

  virtual ~StorageInterface() = default;

  // --- Node operations ---

  //! Insert or update nodes. Each node's id is used as primary key.
  virtual Status UpsertNodes(const std::vector<GraphNode>& nodes) = 0;

  //! Delete nodes by their IDs. Non-existent IDs are silently ignored.
  virtual Status DeleteNodes(const std::vector<std::string>& node_ids) = 0;

  //! Fetch nodes by their IDs. Only found nodes are returned.
  virtual Result<std::vector<GraphNode>> FetchNodes(
      const std::vector<std::string>& node_ids) = 0;

  //! Return all nodes matching a filter expression (e.g. "node_type = 'table'").
  virtual Result<std::vector<GraphNode>> FilterNodes(
      const std::string& filter_expr, int limit = 1000) = 0;

  //! Vector similarity search on nodes.
  virtual Result<std::vector<GraphNode>> QueryNodes(
      const std::string& vector_field, const std::vector<float>& query_vector,
      int topk, const std::string& filter_expr = "") = 0;

  // --- Edge operations ---

  //! Insert or update edges. Each edge's id is used as primary key.
  virtual Status UpsertEdges(const std::vector<GraphEdge>& edges) = 0;

  //! Delete edges by their IDs. Non-existent IDs are silently ignored.
  virtual Status DeleteEdges(const std::vector<std::string>& edge_ids) = 0;

  //! Fetch edges by their IDs. Only found edges are returned.
  virtual Result<std::vector<GraphEdge>> FetchEdges(
      const std::vector<std::string>& edge_ids) = 0;

  //! Return all edges matching a filter expression (e.g. "source_id = 'n1'").
  virtual Result<std::vector<GraphEdge>> FilterEdges(
      const std::string& filter_expr, int limit = 1000) = 0;

  // --- Batch operations ---

  //! Apply a list of mutations. Current implementation executes sequentially;
  //! true RocksDB-level atomicity requires WriteBatch integration (future work).
  virtual Status AtomicBatch(const std::vector<Mutation>& mutations) = 0;

  // --- Index operations ---

  //! Create an index on a field (scalar invert index or vector HNSW index).
  virtual Status CreateIndex(const std::string& collection_name,
                             const std::string& field_name) = 0;

  //! Drop an index from a field.
  virtual Status DropIndex(const std::string& collection_name,
                           const std::string& field_name) = 0;

  // --- Lifecycle ---

  //! Flush pending writes to storage.
  virtual Status Flush() = 0;

  //! Destroy the storage (remove all data from disk).
  virtual Status Destroy() = 0;
};

}  // namespace graph
}  // namespace zvec
