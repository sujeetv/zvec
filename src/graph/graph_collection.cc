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

#include "graph/graph_collection.h"

#include <filesystem>
#include <fstream>

#include "graph/storage/graph_kv_store.h"

namespace zvec {
namespace graph {

static const std::string kMetaFileName = "graph_meta.pb";
static const std::string kKVSubdir = "kv";

std::unique_ptr<GraphCollection> GraphCollection::Create(
    const std::string& path, const GraphSchema& schema) {
  std::string meta_path = path + "/" + kMetaFileName;
  if (std::filesystem::exists(meta_path)) {
    return nullptr;
  }

  std::filesystem::create_directories(path);

  // Serialize schema to protobuf
  auto proto = schema.ToProto();
  std::string serialized;
  if (!proto.SerializeToString(&serialized)) {
    return nullptr;
  }

  std::ofstream out(meta_path, std::ios::binary);
  if (!out.is_open()) {
    return nullptr;
  }
  out.write(serialized.data(), static_cast<std::streamsize>(serialized.size()));
  out.close();

  // Create GraphKVStore
  std::string kv_path = path + "/" + kKVSubdir;
  auto storage = GraphKVStore::Create(kv_path);
  if (!storage) {
    std::filesystem::remove(meta_path);
    return nullptr;
  }

  auto gc = std::unique_ptr<GraphCollection>(new GraphCollection());
  gc->path_ = path;
  gc->schema_ = std::make_unique<GraphSchema>(schema);
  gc->storage_ = std::move(storage);
  gc->mutation_ =
      std::make_unique<MutationEngine>(gc->schema_.get(), gc->storage_.get());
  gc->traversal_ = std::make_unique<TraversalEngine>(gc->storage_.get());

  return gc;
}

std::unique_ptr<GraphCollection> GraphCollection::Open(
    const std::string& path) {
  std::string meta_path = path + "/" + kMetaFileName;

  std::ifstream in(meta_path, std::ios::binary);
  if (!in.is_open()) {
    return nullptr;
  }

  std::string serialized((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
  in.close();

  zvec::graph::proto::GraphSchemaDef proto;
  if (!proto.ParseFromString(serialized)) {
    return nullptr;
  }

  auto schema = GraphSchema::FromProto(proto);

  std::string kv_path = path + "/" + kKVSubdir;
  auto storage = GraphKVStore::Open(kv_path);
  if (!storage) {
    return nullptr;
  }

  auto gc = std::unique_ptr<GraphCollection>(new GraphCollection());
  gc->path_ = path;
  gc->schema_ = std::make_unique<GraphSchema>(std::move(schema));
  gc->storage_ = std::move(storage);
  gc->mutation_ =
      std::make_unique<MutationEngine>(gc->schema_.get(), gc->storage_.get());
  gc->traversal_ = std::make_unique<TraversalEngine>(gc->storage_.get());

  return gc;
}

void GraphCollection::Destroy() {
  if (storage_) {
    storage_->Destroy();
    storage_.reset();
  }
  mutation_.reset();
  traversal_.reset();
  std::filesystem::remove_all(path_);
}

Status GraphCollection::Flush() {
  if (storage_) {
    return storage_->Flush();
  }
  return Status::OK();
}

const GraphSchema& GraphCollection::GetSchema() const { return *schema_; }

// --- Node operations ---

Status GraphCollection::AddNode(const GraphNode& node) {
  return mutation_->AddNode(node);
}

Status GraphCollection::RemoveNode(const std::string& node_id) {
  return mutation_->RemoveNode(node_id);
}

Status GraphCollection::UpdateNode(
    const std::string& node_id,
    const std::unordered_map<std::string, std::string>& properties) {
  return mutation_->UpdateNode(node_id, properties);
}

std::vector<GraphNode> GraphCollection::FetchNodes(
    const std::vector<std::string>& ids) {
  auto result = storage_->FetchNodes(ids);
  return result.has_value() ? std::move(result.value())
                            : std::vector<GraphNode>{};
}

std::vector<GraphNode> GraphCollection::FilterNodes(
    const std::string& filter_expr, int limit) {
  auto result = storage_->FilterNodes(filter_expr, limit);
  return result.has_value() ? std::move(result.value())
                            : std::vector<GraphNode>{};
}

// --- Edge operations ---

Status GraphCollection::AddEdge(
    const std::string& source_id, const std::string& target_id,
    const std::string& edge_type,
    const std::unordered_map<std::string, std::string>& properties) {
  return mutation_->AddEdge(source_id, target_id, edge_type, properties);
}

Status GraphCollection::RemoveEdge(const std::string& edge_id) {
  return mutation_->RemoveEdge(edge_id);
}

Status GraphCollection::UpdateEdge(
    const std::string& edge_id,
    const std::unordered_map<std::string, std::string>& properties) {
  return mutation_->UpdateEdge(edge_id, properties);
}

std::vector<GraphEdge> GraphCollection::FetchEdges(
    const std::vector<std::string>& ids) {
  auto result = storage_->FetchEdges(ids);
  return result.has_value() ? std::move(result.value())
                            : std::vector<GraphEdge>{};
}

std::vector<GraphEdge> GraphCollection::FilterEdges(
    const std::string& filter_expr, int limit) {
  auto result = storage_->FilterEdges(filter_expr, limit);
  return result.has_value() ? std::move(result.value())
                            : std::vector<GraphEdge>{};
}

// --- Traversal ---

Subgraph GraphCollection::Traverse(const TraversalParams& params) {
  return traversal_->Traverse(params);
}

// --- Index operations ---

Status GraphCollection::CreateIndex(const std::string& entity,
                                    const std::string& field) {
  return storage_->CreateIndex(entity, field);
}

Status GraphCollection::DropIndex(const std::string& entity,
                                  const std::string& field) {
  return storage_->DropIndex(entity, field);
}

}  // namespace graph
}  // namespace zvec
