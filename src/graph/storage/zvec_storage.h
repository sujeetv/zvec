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

#include "graph/graph_schema.h"
#include "graph/storage/storage_interface.h"
#include <zvec/db/collection.h>

namespace zvec {
namespace graph {

//! Concrete StorageInterface backed by two zvec Collections (nodes + edges).
//!
//! Nodes collection fields:
//!   _key (primary key = node id), node_type (STRING),
//!   plus all property fields as STRING, plus vector fields.
//!
//! Edges collection fields:
//!   _key (primary key = edge id), source_id (STRING), target_id (STRING),
//!   edge_type (STRING), directed (BOOL),
//!   plus all property fields as STRING.
//!
//! NOTE: AtomicBatch executes mutations sequentially — it is NOT truly atomic
//! at the RocksDB level. A partial failure may leave the store in an
//! inconsistent state. True atomicity requires WriteBatch integration in a
//! future iteration.
class ZvecStorage : public StorageInterface {
 public:
  //! Create a new ZvecStorage, initializing two collections on disk.
  static std::unique_ptr<ZvecStorage> Create(const std::string& path,
                                             const GraphSchema& schema);

  //! Open an existing ZvecStorage from disk.
  static std::unique_ptr<ZvecStorage> Open(const std::string& path);

  ~ZvecStorage() override = default;

  // --- Node operations ---
  Status UpsertNodes(const std::vector<GraphNode>& nodes) override;
  Status DeleteNodes(const std::vector<std::string>& node_ids) override;
  Result<std::vector<GraphNode>> FetchNodes(
      const std::vector<std::string>& node_ids) override;
  Result<std::vector<GraphNode>> FilterNodes(const std::string& filter_expr,
                                             int limit = 1000) override;
  Result<std::vector<GraphNode>> QueryNodes(
      const std::string& vector_field,
      const std::vector<float>& query_vector, int topk,
      const std::string& filter_expr = "") override;

  // --- Edge operations ---
  Status UpsertEdges(const std::vector<GraphEdge>& edges) override;
  Status DeleteEdges(const std::vector<std::string>& edge_ids) override;
  Result<std::vector<GraphEdge>> FetchEdges(
      const std::vector<std::string>& edge_ids) override;
  Result<std::vector<GraphEdge>> FilterEdges(const std::string& filter_expr,
                                             int limit = 1000) override;

  // --- Batch operations ---
  Status AtomicBatch(const std::vector<Mutation>& mutations) override;

  // --- Index operations ---
  Status CreateIndex(const std::string& collection_name,
                     const std::string& field_name) override;
  Status DropIndex(const std::string& collection_name,
                   const std::string& field_name) override;

  // --- Lifecycle ---
  Status Flush() override;
  Status Destroy() override;

 private:
  ZvecStorage() = default;

  //! Build the zvec CollectionSchema for the nodes collection.
  static CollectionSchema BuildNodesSchema(const GraphSchema& schema);

  //! Build the zvec CollectionSchema for the edges collection.
  static CollectionSchema BuildEdgesSchema(const GraphSchema& schema);

  //! Convert a zvec Doc (fetched from nodes collection) to a GraphNode.
  static GraphNode DocToNode(const Doc& doc);

  //! Convert a GraphNode to a zvec Doc for upserting into the nodes collection.
  static Doc NodeToDoc(const GraphNode& node);

  //! Convert a zvec Doc (fetched from edges collection) to a GraphEdge.
  static GraphEdge DocToEdge(const Doc& doc);

  //! Convert a GraphEdge to a zvec Doc for upserting into the edges collection.
  static Doc EdgeToDoc(const GraphEdge& edge);

  std::string path_;
  Collection::Ptr nodes_collection_;
  Collection::Ptr edges_collection_;
};

}  // namespace graph
}  // namespace zvec
