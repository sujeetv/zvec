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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <rocksdb/db.h>

#include "graph/storage/storage_interface.h"

namespace zvec {
namespace graph {

//! RocksDB-backed storage for graph nodes and edges.
//!
//! Uses column families:
//!   "nodes"              — key=node_id, value=GraphNodeProto
//!   "edges"              — key=edge_id, value=GraphEdgeProto
//!   "idx:nodes:<field>"  — secondary indexes on node fields
//!   "idx:edges:<field>"  — secondary indexes on edge fields
//!
//! All reads use MultiGet (lock-free, concurrent).
//! All writes use WriteBatch (truly atomic).
class GraphKVStore : public StorageInterface {
 public:
  ~GraphKVStore() override;

  //! Create a new KV store at the given path with auto-indexes.
  static std::unique_ptr<GraphKVStore> Create(const std::string& path);

  //! Open an existing KV store.
  static std::unique_ptr<GraphKVStore> Open(const std::string& path);

  // --- StorageInterface implementation ---

  Status UpsertNodes(const std::vector<GraphNode>& nodes) override;
  Status DeleteNodes(const std::vector<std::string>& node_ids) override;
  Result<std::vector<GraphNode>> FetchNodes(
      const std::vector<std::string>& node_ids) override;
  Result<std::vector<GraphNode>> FetchNodesLite(
      const std::vector<std::string>& node_ids) override;
  Result<std::vector<GraphNode>> FilterNodes(const std::string& filter_expr,
                                             int limit = 1000) override;

  // QueryNodes is not supported — vector search is separate.
  Result<std::vector<GraphNode>> QueryNodes(
      const std::string& vector_field,
      const std::vector<float>& query_vector, int topk,
      const std::string& filter_expr = "") override;

  Status UpsertEdges(const std::vector<GraphEdge>& edges) override;
  Status DeleteEdges(const std::vector<std::string>& edge_ids) override;
  Result<std::vector<GraphEdge>> FetchEdges(
      const std::vector<std::string>& edge_ids) override;
  Result<std::vector<GraphEdge>> FilterEdges(const std::string& filter_expr,
                                             int limit = 1000) override;

  Status AtomicBatch(const std::vector<Mutation>& mutations) override;

  Status CreateIndex(const std::string& collection_name,
                     const std::string& field_name) override;
  Status DropIndex(const std::string& collection_name,
                   const std::string& field_name) override;

  Status Flush() override;
  Status Destroy() override;

 private:
  GraphKVStore() = default;

  std::string path_;
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::ColumnFamilyHandle* nodes_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* edges_cf_ = nullptr;
  std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> index_cfs_;
  std::vector<rocksdb::ColumnFamilyHandle*> all_cf_handles_;

  //! Shared merge operator instance for all index CFs.
  std::shared_ptr<rocksdb::MergeOperator> merge_op_;

  //! Mutex for index CF creation/deletion (not for reads/writes).
  std::mutex index_mutex_;

  //! Serialize a GraphNode to protobuf bytes.
  static std::string SerializeNode(const GraphNode& node);

  //! Deserialize a GraphNode from protobuf bytes.
  static GraphNode DeserializeNode(const rocksdb::Slice& data);

  //! Deserialize a GraphNode (lite — skip properties).
  static GraphNode DeserializeNodeLite(const rocksdb::Slice& data);

  //! Serialize a GraphEdge to protobuf bytes.
  static std::string SerializeEdge(const GraphEdge& edge);

  //! Deserialize a GraphEdge from protobuf bytes.
  static GraphEdge DeserializeEdge(const rocksdb::Slice& data);

  //! Add index merge operations for a node to a WriteBatch.
  void AddNodeIndexOps(rocksdb::WriteBatch& batch, const GraphNode& node,
                       bool is_add);

  //! Add index merge operations for an edge to a WriteBatch.
  void AddEdgeIndexOps(rocksdb::WriteBatch& batch, const GraphEdge& edge,
                       bool is_add);

  //! Get or null for an index CF handle.
  rocksdb::ColumnFamilyHandle* GetIndexCF(const std::string& name);

  //! Parse a simple "field = 'value'" filter.
  static bool ParseFilter(const std::string& filter, std::string& field,
                           std::string& value);

  //! Configure RocksDB options for graph workloads.
  static rocksdb::Options MakeOptions(
      std::shared_ptr<rocksdb::MergeOperator> merge_op);
};

}  // namespace graph
}  // namespace zvec
