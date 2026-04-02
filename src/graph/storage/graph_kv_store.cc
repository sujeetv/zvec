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

#include "graph/storage/graph_kv_store.h"

#include <algorithm>
#include <filesystem>

#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

#include <zvec/ailego/pattern/expected.hpp>

#include "graph/proto/graph.pb.h"
#include "graph/storage/index_merge_operator.h"

namespace zvec {
namespace graph {

// Auto-created index column families.
static const std::vector<std::string> kAutoIndexes = {
    "idx:nodes:node_type",
    "idx:edges:source_id",
    "idx:edges:target_id",
    "idx:edges:edge_type",
};

GraphKVStore::~GraphKVStore() {
  if (db_) {
    for (auto* handle : all_cf_handles_) {
      db_->DestroyColumnFamilyHandle(handle);
    }
    db_->Close();
  }
}

rocksdb::Options GraphKVStore::MakeOptions(
    std::shared_ptr<rocksdb::MergeOperator> merge_op) {
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.create_missing_column_families = true;
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();

  // Block-based table with bloom filter and LRU cache
  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_opts.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);  // 64MB
  opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

  // Compression: LZ4 for speed
  opts.compression = rocksdb::kLZ4Compression;

  // Merge operator for index CFs
  if (merge_op) {
    opts.merge_operator = merge_op;
  }

  // Log level
  opts.info_log_level = rocksdb::WARN_LEVEL;
  opts.keep_log_file_num = 1;

  return opts;
}

std::unique_ptr<GraphKVStore> GraphKVStore::Create(const std::string& path) {
  std::filesystem::create_directories(path);

  auto store = std::unique_ptr<GraphKVStore>(new GraphKVStore());
  store->path_ = path;
  store->merge_op_ = std::make_shared<IndexMergeOperator>();

  auto opts = MakeOptions(store->merge_op_);

  // Build column family descriptors
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(rocksdb::kDefaultColumnFamilyName,
                        rocksdb::ColumnFamilyOptions(opts));

  // Data CFs (no merge operator needed)
  rocksdb::ColumnFamilyOptions data_cf_opts(opts);
  data_cf_opts.merge_operator = nullptr;
  cf_descs.emplace_back("nodes", data_cf_opts);
  cf_descs.emplace_back("edges", data_cf_opts);

  // Index CFs (with merge operator)
  rocksdb::ColumnFamilyOptions idx_cf_opts(opts);
  for (const auto& idx : kAutoIndexes) {
    cf_descs.emplace_back(idx, idx_cf_opts);
  }

  rocksdb::DB* db_raw = nullptr;
  auto status = rocksdb::DB::Open(opts, path, cf_descs,
                                  &store->all_cf_handles_, &db_raw);
  if (!status.ok()) {
    return nullptr;
  }
  store->db_.reset(db_raw);

  // Map handles: default=0, nodes=1, edges=2, indexes=3..
  store->nodes_cf_ = store->all_cf_handles_[1];
  store->edges_cf_ = store->all_cf_handles_[2];
  for (size_t i = 3; i < store->all_cf_handles_.size(); ++i) {
    store->index_cfs_[store->all_cf_handles_[i]->GetName()] =
        store->all_cf_handles_[i];
  }

  return store;
}

std::unique_ptr<GraphKVStore> GraphKVStore::Open(const std::string& path) {
  auto store = std::unique_ptr<GraphKVStore>(new GraphKVStore());
  store->path_ = path;
  store->merge_op_ = std::make_shared<IndexMergeOperator>();

  auto opts = MakeOptions(store->merge_op_);
  opts.create_if_missing = false;

  // List existing column families
  std::vector<std::string> cf_names;
  auto s = rocksdb::DB::ListColumnFamilies(opts, path, &cf_names);
  if (!s.ok()) {
    return nullptr;
  }

  // Open all CFs
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
  rocksdb::ColumnFamilyOptions data_cf_opts(opts);
  data_cf_opts.merge_operator = nullptr;
  rocksdb::ColumnFamilyOptions idx_cf_opts(opts);

  for (const auto& name : cf_names) {
    if (name == "nodes" || name == "edges" ||
        name == rocksdb::kDefaultColumnFamilyName) {
      cf_descs.emplace_back(name, data_cf_opts);
    } else {
      // Index CFs get the merge operator
      cf_descs.emplace_back(name, idx_cf_opts);
    }
  }

  rocksdb::DB* db_raw = nullptr;
  s = rocksdb::DB::Open(opts, path, cf_descs, &store->all_cf_handles_,
                         &db_raw);
  if (!s.ok()) {
    return nullptr;
  }
  store->db_.reset(db_raw);

  // Map handles by name
  for (size_t i = 0; i < cf_names.size(); ++i) {
    const auto& name = cf_names[i];
    auto* handle = store->all_cf_handles_[i];
    if (name == "nodes") {
      store->nodes_cf_ = handle;
    } else if (name == "edges") {
      store->edges_cf_ = handle;
    } else if (name != rocksdb::kDefaultColumnFamilyName) {
      store->index_cfs_[name] = handle;
    }
  }

  return store;
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

std::string GraphKVStore::SerializeNode(const GraphNode& node) {
  proto::GraphNodeProto pb;
  pb.set_id(node.id);
  pb.set_node_type(node.node_type);
  for (const auto& [k, v] : node.properties) {
    (*pb.mutable_properties())[k] = v;
  }
  for (const auto& id : node.neighbor_ids) {
    pb.add_neighbor_ids(id);
  }
  for (const auto& id : node.neighbor_edge_ids) {
    pb.add_neighbor_edge_ids(id);
  }
  pb.set_version(node.version);
  pb.set_updated_at(node.updated_at);
  return pb.SerializeAsString();
}

GraphNode GraphKVStore::DeserializeNode(const rocksdb::Slice& data) {
  proto::GraphNodeProto pb;
  pb.ParseFromArray(data.data(), static_cast<int>(data.size()));

  GraphNode node;
  node.id = pb.id();
  node.node_type = pb.node_type();
  for (const auto& [k, v] : pb.properties()) {
    node.properties[k] = v;
  }
  node.neighbor_ids.reserve(pb.neighbor_ids_size());
  for (const auto& id : pb.neighbor_ids()) {
    node.neighbor_ids.push_back(id);
  }
  node.neighbor_edge_ids.reserve(pb.neighbor_edge_ids_size());
  for (const auto& id : pb.neighbor_edge_ids()) {
    node.neighbor_edge_ids.push_back(id);
  }
  node.version = pb.version();
  node.updated_at = pb.updated_at();
  return node;
}

GraphNode GraphKVStore::DeserializeNodeLite(const rocksdb::Slice& data) {
  proto::GraphNodeProto pb;
  pb.ParseFromArray(data.data(), static_cast<int>(data.size()));

  GraphNode node;
  node.id = pb.id();
  node.node_type = pb.node_type();
  // Skip properties — traversal only needs adjacency
  node.neighbor_ids.reserve(pb.neighbor_ids_size());
  for (const auto& id : pb.neighbor_ids()) {
    node.neighbor_ids.push_back(id);
  }
  node.neighbor_edge_ids.reserve(pb.neighbor_edge_ids_size());
  for (const auto& id : pb.neighbor_edge_ids()) {
    node.neighbor_edge_ids.push_back(id);
  }
  node.version = pb.version();
  node.updated_at = pb.updated_at();
  return node;
}

std::string GraphKVStore::SerializeEdge(const GraphEdge& edge) {
  proto::GraphEdgeProto pb;
  pb.set_id(edge.id);
  pb.set_source_id(edge.source_id);
  pb.set_target_id(edge.target_id);
  pb.set_edge_type(edge.edge_type);
  pb.set_directed(edge.directed);
  for (const auto& [k, v] : edge.properties) {
    (*pb.mutable_properties())[k] = v;
  }
  pb.set_version(edge.version);
  pb.set_updated_at(edge.updated_at);
  return pb.SerializeAsString();
}

GraphEdge GraphKVStore::DeserializeEdge(const rocksdb::Slice& data) {
  proto::GraphEdgeProto pb;
  pb.ParseFromArray(data.data(), static_cast<int>(data.size()));

  GraphEdge edge;
  edge.id = pb.id();
  edge.source_id = pb.source_id();
  edge.target_id = pb.target_id();
  edge.edge_type = pb.edge_type();
  edge.directed = pb.directed();
  for (const auto& [k, v] : pb.properties()) {
    edge.properties[k] = v;
  }
  edge.version = pb.version();
  edge.updated_at = pb.updated_at();
  return edge;
}

// ---------------------------------------------------------------------------
// Index helpers
// ---------------------------------------------------------------------------

rocksdb::ColumnFamilyHandle* GraphKVStore::GetIndexCF(
    const std::string& name) {
  auto it = index_cfs_.find(name);
  return (it != index_cfs_.end()) ? it->second : nullptr;
}

void GraphKVStore::AddNodeIndexOps(rocksdb::WriteBatch& batch,
                                   const GraphNode& node, bool is_add) {
  auto encode = is_add ? IndexMergeOperator::EncodeAdd
                       : IndexMergeOperator::EncodeRemove;

  // node_type index (always present)
  auto* cf = GetIndexCF("idx:nodes:node_type");
  if (cf) {
    batch.Merge(cf, node.node_type, encode(node.id));
  }

  // User-defined property indexes
  for (const auto& [prop, val] : node.properties) {
    std::string idx_name = "idx:nodes:" + prop;
    cf = GetIndexCF(idx_name);
    if (cf) {
      batch.Merge(cf, val, encode(node.id));
    }
  }
}

void GraphKVStore::AddEdgeIndexOps(rocksdb::WriteBatch& batch,
                                   const GraphEdge& edge, bool is_add) {
  auto encode = is_add ? IndexMergeOperator::EncodeAdd
                       : IndexMergeOperator::EncodeRemove;

  auto* cf = GetIndexCF("idx:edges:source_id");
  if (cf) batch.Merge(cf, edge.source_id, encode(edge.id));

  cf = GetIndexCF("idx:edges:target_id");
  if (cf) batch.Merge(cf, edge.target_id, encode(edge.id));

  cf = GetIndexCF("idx:edges:edge_type");
  if (cf) batch.Merge(cf, edge.edge_type, encode(edge.id));

  // User-defined property indexes
  for (const auto& [prop, val] : edge.properties) {
    std::string idx_name = "idx:edges:" + prop;
    cf = GetIndexCF(idx_name);
    if (cf) {
      batch.Merge(cf, val, encode(edge.id));
    }
  }
}

bool GraphKVStore::ParseFilter(const std::string& filter, std::string& field,
                                std::string& value) {
  auto eq_pos = filter.find('=');
  if (eq_pos == std::string::npos) return false;

  field = filter.substr(0, eq_pos);
  while (!field.empty() && field.back() == ' ') field.pop_back();
  while (!field.empty() && field.front() == ' ') field = field.substr(1);

  value = filter.substr(eq_pos + 1);
  while (!value.empty() && value.front() == ' ') value = value.substr(1);
  while (!value.empty() && value.back() == ' ') value.pop_back();

  if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
    value = value.substr(1, value.size() - 2);
  }

  return !field.empty() && !value.empty();
}

// ---------------------------------------------------------------------------
// Node operations
// ---------------------------------------------------------------------------

Status GraphKVStore::UpsertNodes(const std::vector<GraphNode>& nodes) {
  rocksdb::WriteBatch batch;
  for (const auto& node : nodes) {
    batch.Put(nodes_cf_, node.id, SerializeNode(node));
    AddNodeIndexOps(batch, node, true);
  }

  rocksdb::WriteOptions write_opts;
  auto s = db_->Write(write_opts, &batch);
  if (!s.ok()) {
    return Status::InternalError("RocksDB write failed: ", s.ToString());
  }
  return Status::OK();
}

Status GraphKVStore::DeleteNodes(const std::vector<std::string>& node_ids) {
  // Fetch nodes first to clean up indexes
  auto result = FetchNodes(node_ids);
  if (!result.has_value()) {
    return Status::OK();  // Nothing to delete
  }

  rocksdb::WriteBatch batch;
  for (const auto& node : result.value()) {
    batch.Delete(nodes_cf_, node.id);
    AddNodeIndexOps(batch, node, false);
  }

  rocksdb::WriteOptions write_opts;
  auto s = db_->Write(write_opts, &batch);
  if (!s.ok()) {
    return Status::InternalError("RocksDB write failed: ", s.ToString());
  }
  return Status::OK();
}

Result<std::vector<GraphNode>> GraphKVStore::FetchNodes(
    const std::vector<std::string>& node_ids) {
  if (node_ids.empty()) {
    return std::vector<GraphNode>{};
  }

  rocksdb::ReadOptions read_opts;
  std::vector<rocksdb::Slice> keys;
  keys.reserve(node_ids.size());
  for (const auto& id : node_ids) {
    keys.emplace_back(id);
  }

  std::vector<rocksdb::ColumnFamilyHandle*> cfs(keys.size(), nodes_cf_);
  std::vector<std::string> values(keys.size());
  std::vector<rocksdb::Status> statuses =
      db_->MultiGet(read_opts, cfs, keys, &values);

  std::vector<GraphNode> nodes;
  nodes.reserve(node_ids.size());
  for (size_t i = 0; i < statuses.size(); ++i) {
    if (statuses[i].ok()) {
      nodes.push_back(DeserializeNode(values[i]));
    }
  }

  return nodes;
}

Result<std::vector<GraphNode>> GraphKVStore::FetchNodesLite(
    const std::vector<std::string>& node_ids) {
  if (node_ids.empty()) {
    return std::vector<GraphNode>{};
  }

  rocksdb::ReadOptions read_opts;
  std::vector<rocksdb::Slice> keys;
  keys.reserve(node_ids.size());
  for (const auto& id : node_ids) {
    keys.emplace_back(id);
  }

  std::vector<rocksdb::ColumnFamilyHandle*> cfs(keys.size(), nodes_cf_);
  std::vector<std::string> values(keys.size());
  std::vector<rocksdb::Status> statuses =
      db_->MultiGet(read_opts, cfs, keys, &values);

  std::vector<GraphNode> nodes;
  nodes.reserve(node_ids.size());
  for (size_t i = 0; i < statuses.size(); ++i) {
    if (statuses[i].ok()) {
      nodes.push_back(DeserializeNodeLite(values[i]));
    }
  }

  return nodes;
}

Result<std::vector<GraphNode>> GraphKVStore::FilterNodes(
    const std::string& filter_expr, int limit) {
  std::string field, value;
  if (!ParseFilter(filter_expr, field, value)) {
    return tl::unexpected<Status>(
        Status::InvalidArgument("Invalid filter expression: ", filter_expr));
  }

  // Try index lookup
  std::string idx_name = "idx:nodes:" + field;
  auto* cf = GetIndexCF(idx_name);
  if (cf) {
    // Read from index
    rocksdb::ReadOptions read_opts;
    std::string raw;
    auto s = db_->Get(read_opts, cf, value, &raw);
    if (!s.ok()) {
      return std::vector<GraphNode>{};
    }

    proto::IndexEntry entry;
    if (!entry.ParseFromString(raw)) {
      return std::vector<GraphNode>{};
    }

    // Fetch matching nodes
    std::vector<std::string> ids;
    int count = std::min(static_cast<int>(entry.ids_size()), limit);
    ids.reserve(count);
    for (int i = 0; i < count; ++i) {
      ids.push_back(entry.ids(i));
    }

    return FetchNodes(ids);
  }

  // Fallback: full scan over nodes CF
  std::vector<GraphNode> results;
  rocksdb::ReadOptions read_opts;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts, nodes_cf_));
  for (it->SeekToFirst(); it->Valid() && static_cast<int>(results.size()) < limit;
       it->Next()) {
    GraphNode node = DeserializeNode(it->value());
    // Check filter match
    if (field == "node_type" && node.node_type == value) {
      results.push_back(std::move(node));
    } else if (field == "id" && node.id == value) {
      results.push_back(std::move(node));
    } else {
      auto pit = node.properties.find(field);
      if (pit != node.properties.end() && pit->second == value) {
        results.push_back(std::move(node));
      }
    }
  }

  return results;
}

Result<std::vector<GraphNode>> GraphKVStore::QueryNodes(
    const std::string& /*vector_field*/,
    const std::vector<float>& /*query_vector*/, int /*topk*/,
    const std::string& /*filter_expr*/) {
  return tl::unexpected<Status>(Status::InvalidArgument(
      "QueryNodes not supported on GraphKVStore. Use a zvec Collection for "
      "vector search."));
}

// ---------------------------------------------------------------------------
// Edge operations
// ---------------------------------------------------------------------------

Status GraphKVStore::UpsertEdges(const std::vector<GraphEdge>& edges) {
  rocksdb::WriteBatch batch;
  for (const auto& edge : edges) {
    batch.Put(edges_cf_, edge.id, SerializeEdge(edge));
    AddEdgeIndexOps(batch, edge, true);
  }

  rocksdb::WriteOptions write_opts;
  auto s = db_->Write(write_opts, &batch);
  if (!s.ok()) {
    return Status::InternalError("RocksDB write failed: ", s.ToString());
  }
  return Status::OK();
}

Status GraphKVStore::DeleteEdges(const std::vector<std::string>& edge_ids) {
  auto result = FetchEdges(edge_ids);
  if (!result.has_value()) {
    return Status::OK();
  }

  rocksdb::WriteBatch batch;
  for (const auto& edge : result.value()) {
    batch.Delete(edges_cf_, edge.id);
    AddEdgeIndexOps(batch, edge, false);
  }

  rocksdb::WriteOptions write_opts;
  auto s = db_->Write(write_opts, &batch);
  if (!s.ok()) {
    return Status::InternalError("RocksDB write failed: ", s.ToString());
  }
  return Status::OK();
}

Result<std::vector<GraphEdge>> GraphKVStore::FetchEdges(
    const std::vector<std::string>& edge_ids) {
  if (edge_ids.empty()) {
    return std::vector<GraphEdge>{};
  }

  rocksdb::ReadOptions read_opts;
  std::vector<rocksdb::Slice> keys;
  keys.reserve(edge_ids.size());
  for (const auto& id : edge_ids) {
    keys.emplace_back(id);
  }

  std::vector<rocksdb::ColumnFamilyHandle*> cfs(keys.size(), edges_cf_);
  std::vector<std::string> values(keys.size());
  std::vector<rocksdb::Status> statuses =
      db_->MultiGet(read_opts, cfs, keys, &values);

  std::vector<GraphEdge> edges;
  edges.reserve(edge_ids.size());
  for (size_t i = 0; i < statuses.size(); ++i) {
    if (statuses[i].ok()) {
      edges.push_back(DeserializeEdge(values[i]));
    }
  }

  return edges;
}

Result<std::vector<GraphEdge>> GraphKVStore::FilterEdges(
    const std::string& filter_expr, int limit) {
  std::string field, value;
  if (!ParseFilter(filter_expr, field, value)) {
    return tl::unexpected<Status>(
        Status::InvalidArgument("Invalid filter expression: ", filter_expr));
  }

  // Try index lookup
  std::string idx_name = "idx:edges:" + field;
  auto* cf = GetIndexCF(idx_name);
  if (cf) {
    rocksdb::ReadOptions read_opts;
    std::string raw;
    auto s = db_->Get(read_opts, cf, value, &raw);
    if (!s.ok()) {
      return std::vector<GraphEdge>{};
    }

    proto::IndexEntry entry;
    if (!entry.ParseFromString(raw)) {
      return std::vector<GraphEdge>{};
    }

    std::vector<std::string> ids;
    int count = std::min(static_cast<int>(entry.ids_size()), limit);
    ids.reserve(count);
    for (int i = 0; i < count; ++i) {
      ids.push_back(entry.ids(i));
    }

    return FetchEdges(ids);
  }

  // Fallback: full scan
  std::vector<GraphEdge> results;
  rocksdb::ReadOptions read_opts;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts, edges_cf_));
  for (it->SeekToFirst(); it->Valid() && static_cast<int>(results.size()) < limit;
       it->Next()) {
    GraphEdge edge = DeserializeEdge(it->value());
    if ((field == "edge_type" && edge.edge_type == value) ||
        (field == "source_id" && edge.source_id == value) ||
        (field == "target_id" && edge.target_id == value) ||
        (field == "id" && edge.id == value)) {
      results.push_back(std::move(edge));
    } else {
      auto pit = edge.properties.find(field);
      if (pit != edge.properties.end() && pit->second == value) {
        results.push_back(std::move(edge));
      }
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// Atomic batch
// ---------------------------------------------------------------------------

Status GraphKVStore::AtomicBatch(const std::vector<Mutation>& mutations) {
  rocksdb::WriteBatch batch;

  for (const auto& m : mutations) {
    switch (m.type) {
      case MutationType::UPSERT_NODE:
        batch.Put(nodes_cf_, m.node.id, SerializeNode(m.node));
        AddNodeIndexOps(batch, m.node, true);
        break;
      case MutationType::DELETE_NODE:
        batch.Delete(nodes_cf_, m.node.id);
        AddNodeIndexOps(batch, m.node, false);
        break;
      case MutationType::UPSERT_EDGE:
        batch.Put(edges_cf_, m.edge.id, SerializeEdge(m.edge));
        AddEdgeIndexOps(batch, m.edge, true);
        break;
      case MutationType::DELETE_EDGE:
        batch.Delete(edges_cf_, m.edge.id);
        AddEdgeIndexOps(batch, m.edge, false);
        break;
    }
  }

  rocksdb::WriteOptions write_opts;
  auto s = db_->Write(write_opts, &batch);
  if (!s.ok()) {
    return Status::InternalError("RocksDB write failed: ", s.ToString());
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// Index operations
// ---------------------------------------------------------------------------

Status GraphKVStore::CreateIndex(const std::string& collection_name,
                                 const std::string& field_name) {
  std::lock_guard<std::mutex> lock(index_mutex_);

  std::string idx_name = "idx:" + collection_name + ":" + field_name;
  if (GetIndexCF(idx_name)) {
    return Status::OK();  // Already exists
  }

  // Create column family with merge operator
  rocksdb::ColumnFamilyOptions cf_opts;
  cf_opts.merge_operator = merge_op_;
  rocksdb::ColumnFamilyHandle* handle = nullptr;
  auto s = db_->CreateColumnFamily(cf_opts, idx_name, &handle);
  if (!s.ok()) {
    return Status::InternalError("Failed to create index CF: ", s.ToString());
  }

  all_cf_handles_.push_back(handle);
  index_cfs_[idx_name] = handle;

  // Backfill: scan existing data and populate the index
  bool is_nodes = (collection_name == "nodes");
  auto* data_cf = is_nodes ? nodes_cf_ : edges_cf_;

  rocksdb::ReadOptions read_opts;
  rocksdb::WriteBatch batch;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts, data_cf));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (is_nodes) {
      GraphNode node = DeserializeNode(it->value());
      std::string val;
      if (field_name == "node_type") {
        val = node.node_type;
      } else {
        auto pit = node.properties.find(field_name);
        if (pit != node.properties.end()) {
          val = pit->second;
        }
      }
      if (!val.empty()) {
        batch.Merge(handle, val, IndexMergeOperator::EncodeAdd(node.id));
      }
    } else {
      GraphEdge edge = DeserializeEdge(it->value());
      std::string val;
      if (field_name == "source_id") val = edge.source_id;
      else if (field_name == "target_id") val = edge.target_id;
      else if (field_name == "edge_type") val = edge.edge_type;
      else {
        auto pit = edge.properties.find(field_name);
        if (pit != edge.properties.end()) val = pit->second;
      }
      if (!val.empty()) {
        batch.Merge(handle, val, IndexMergeOperator::EncodeAdd(edge.id));
      }
    }
  }

  rocksdb::WriteOptions write_opts;
  s = db_->Write(write_opts, &batch);
  if (!s.ok()) {
    return Status::InternalError("Failed to backfill index: ", s.ToString());
  }

  return Status::OK();
}

Status GraphKVStore::DropIndex(const std::string& collection_name,
                               const std::string& field_name) {
  std::lock_guard<std::mutex> lock(index_mutex_);

  std::string idx_name = "idx:" + collection_name + ":" + field_name;
  auto it = index_cfs_.find(idx_name);
  if (it == index_cfs_.end()) {
    return Status::OK();
  }

  auto* handle = it->second;
  auto s = db_->DropColumnFamily(handle);
  if (!s.ok()) {
    return Status::InternalError("Failed to drop index CF: ", s.ToString());
  }

  db_->DestroyColumnFamilyHandle(handle);
  all_cf_handles_.erase(
      std::remove(all_cf_handles_.begin(), all_cf_handles_.end(), handle),
      all_cf_handles_.end());
  index_cfs_.erase(it);

  return Status::OK();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

Status GraphKVStore::Flush() {
  rocksdb::FlushOptions flush_opts;
  auto s = db_->Flush(flush_opts);
  if (!s.ok()) {
    return Status::InternalError("RocksDB flush failed: ", s.ToString());
  }
  return Status::OK();
}

Status GraphKVStore::Destroy() {
  // Close DB first
  for (auto* handle : all_cf_handles_) {
    db_->DestroyColumnFamilyHandle(handle);
  }
  all_cf_handles_.clear();
  index_cfs_.clear();
  nodes_cf_ = nullptr;
  edges_cf_ = nullptr;
  db_->Close();
  db_.reset();

  // Remove all files
  std::filesystem::remove_all(path_);
  return Status::OK();
}

}  // namespace graph
}  // namespace zvec
