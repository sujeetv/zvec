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

#include "graph/graph_engine.h"

#include <filesystem>
#include <fstream>

#include "graph/storage/zvec_storage.h"

namespace zvec {
namespace graph {

static const std::string kMetaFileName = "graph_meta.pb";

std::unique_ptr<GraphEngine> GraphEngine::Create(const std::string& path,
                                                 const GraphSchema& schema) {
  // Fail if the metadata file already exists (graph already created).
  std::string meta_path = path + "/" + kMetaFileName;
  if (std::filesystem::exists(meta_path)) {
    return nullptr;
  }

  // Create the directory tree if it doesn't exist.
  std::filesystem::create_directories(path);

  // Serialize schema to protobuf and write to disk.
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

  // Create storage (nodes + edges collections).
  auto storage = ZvecStorage::Create(path, schema);
  if (!storage) {
    // Clean up the metadata file on failure.
    std::filesystem::remove(meta_path);
    return nullptr;
  }

  auto engine = std::unique_ptr<GraphEngine>(new GraphEngine());
  engine->path_ = path;
  engine->schema_ = std::make_unique<GraphSchema>(schema);
  engine->storage_ = std::move(storage);
  engine->mutation_ =
      std::make_unique<MutationEngine>(engine->schema_.get(), engine->storage_.get());
  engine->traversal_ =
      std::make_unique<TraversalEngine>(engine->storage_.get());

  return engine;
}

std::unique_ptr<GraphEngine> GraphEngine::Open(const std::string& path) {
  std::string meta_path = path + "/" + kMetaFileName;

  // Read the metadata file.
  std::ifstream in(meta_path, std::ios::binary);
  if (!in.is_open()) {
    return nullptr;
  }

  std::string serialized((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
  in.close();

  // Deserialize schema from protobuf.
  zvec::graph::proto::GraphSchemaDef proto;
  if (!proto.ParseFromString(serialized)) {
    return nullptr;
  }

  auto schema = GraphSchema::FromProto(proto);

  // Open existing storage.
  auto storage = ZvecStorage::Open(path);
  if (!storage) {
    return nullptr;
  }

  auto engine = std::unique_ptr<GraphEngine>(new GraphEngine());
  engine->path_ = path;
  engine->schema_ = std::make_unique<GraphSchema>(std::move(schema));
  engine->storage_ = std::move(storage);
  engine->mutation_ =
      std::make_unique<MutationEngine>(engine->schema_.get(), engine->storage_.get());
  engine->traversal_ =
      std::make_unique<TraversalEngine>(engine->storage_.get());

  return engine;
}

void GraphEngine::Destroy() {
  // Destroy underlying storage first.
  if (storage_) {
    storage_->Destroy();
    storage_.reset();
  }
  mutation_.reset();
  traversal_.reset();

  // Remove the entire directory tree.
  std::filesystem::remove_all(path_);
}

Status GraphEngine::Repair() {
  // Stub: full orphan detection is a future enhancement.
  return Status::OK();
}

const GraphSchema& GraphEngine::GetSchema() const { return *schema_; }

// ---------------------------------------------------------------------------
// Node operations — delegate to MutationEngine / StorageInterface
// ---------------------------------------------------------------------------

Status GraphEngine::AddNode(const GraphNode& node) {
  return mutation_->AddNode(node);
}

Status GraphEngine::RemoveNode(const std::string& node_id) {
  return mutation_->RemoveNode(node_id);
}

Status GraphEngine::UpdateNode(
    const std::string& node_id,
    const std::unordered_map<std::string, std::string>& properties) {
  return mutation_->UpdateNode(node_id, properties);
}

std::vector<GraphNode> GraphEngine::FetchNodes(
    const std::vector<std::string>& ids) {
  auto result = storage_->FetchNodes(ids);
  if (result.has_value()) {
    return std::move(result.value());
  }
  return {};
}

// ---------------------------------------------------------------------------
// Edge operations — delegate to MutationEngine / StorageInterface
// ---------------------------------------------------------------------------

Status GraphEngine::AddEdge(
    const std::string& source_id, const std::string& target_id,
    const std::string& edge_type,
    const std::unordered_map<std::string, std::string>& properties) {
  return mutation_->AddEdge(source_id, target_id, edge_type, properties);
}

Status GraphEngine::RemoveEdge(const std::string& edge_id) {
  return mutation_->RemoveEdge(edge_id);
}

std::vector<GraphEdge> GraphEngine::FetchEdges(
    const std::vector<std::string>& ids) {
  auto result = storage_->FetchEdges(ids);
  if (result.has_value()) {
    return std::move(result.value());
  }
  return {};
}

// ---------------------------------------------------------------------------
// Traversal — delegate to TraversalEngine
// ---------------------------------------------------------------------------

Subgraph GraphEngine::Traverse(const TraversalParams& params) {
  return traversal_->Traverse(params);
}

}  // namespace graph
}  // namespace zvec
