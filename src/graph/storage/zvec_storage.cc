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

#include "graph/storage/zvec_storage.h"

#include <sstream>

#include <zvec/ailego/logger/logger.h>
#include <zvec/ailego/utility/file_helper.h>
#include <zvec/db/index_params.h>

namespace zvec {
namespace graph {

// zvec requires at least one vector field per collection. When no user-defined
// vector fields exist we add a small placeholder that is never queried.
static constexpr const char* kPlaceholderVectorField = "_placeholder_vec";
static constexpr uint32_t kPlaceholderVectorDim = 8;
static const std::string kVectorPropertyPrefix = "_vec_";

// ---------------------------------------------------------------------------
// Schema builders
// ---------------------------------------------------------------------------

CollectionSchema ZvecStorage::BuildNodesSchema(const GraphSchema& schema) {
  CollectionSchema cs("graph_nodes");

  // node_type — filterable string
  cs.add_field(std::make_shared<FieldSchema>("node_type", DataType::STRING,
                                             /*nullable=*/false));

  // Collect all property and vector names across every node type.
  std::unordered_set<std::string> prop_names;
  std::unordered_set<std::string> vec_names;

  auto proto = schema.ToProto();
  for (int i = 0; i < proto.node_types_size(); ++i) {
    const auto& nt = proto.node_types(i);
    for (int j = 0; j < nt.properties_size(); ++j) {
      prop_names.insert(nt.properties(j).name());
    }
    for (int j = 0; j < nt.vectors_size(); ++j) {
      vec_names.insert(nt.vectors(j).name());
    }
  }

  for (const auto& name : prop_names) {
    cs.add_field(std::make_shared<FieldSchema>(name, DataType::STRING,
                                               /*nullable=*/true));
  }

  // User-defined vectors are stored as serialized STRING properties with a
  // prefix. This avoids issues with nullable vector columns in zvec.
  for (const auto& name : vec_names) {
    cs.add_field(std::make_shared<FieldSchema>(
        kVectorPropertyPrefix + name, DataType::STRING, /*nullable=*/true));
  }

  // System fields for mutation tracking.
  cs.add_field(std::make_shared<FieldSchema>("_version", DataType::UINT64,
                                             /*nullable=*/true));
  cs.add_field(std::make_shared<FieldSchema>("_updated_at", DataType::UINT64,
                                             /*nullable=*/true));

  // Adjacency lists stored as array-of-string fields.
  cs.add_field(std::make_shared<FieldSchema>("_neighbor_ids",
                                             DataType::ARRAY_STRING,
                                             /*nullable=*/true));
  cs.add_field(std::make_shared<FieldSchema>("_neighbor_edge_ids",
                                             DataType::ARRAY_STRING,
                                             /*nullable=*/true));

  // zvec requires at least one vector field per collection. We use a small
  // placeholder that is always populated.
  cs.add_field(std::make_shared<FieldSchema>(
      kPlaceholderVectorField, DataType::VECTOR_FP32, kPlaceholderVectorDim,
      /*nullable=*/false,
      std::make_shared<FlatIndexParams>(MetricType::IP)));

  return cs;
}

CollectionSchema ZvecStorage::BuildEdgesSchema(const GraphSchema& schema) {
  CollectionSchema cs("graph_edges");

  // Core edge metadata fields.
  cs.add_field(std::make_shared<FieldSchema>("source_id", DataType::STRING,
                                             /*nullable=*/false));
  cs.add_field(std::make_shared<FieldSchema>("target_id", DataType::STRING,
                                             /*nullable=*/false));
  cs.add_field(std::make_shared<FieldSchema>("edge_type", DataType::STRING,
                                             /*nullable=*/false));
  cs.add_field(std::make_shared<FieldSchema>("directed", DataType::BOOL,
                                             /*nullable=*/false));

  // Collect all property names across every edge type.
  std::unordered_set<std::string> prop_names;
  auto proto = schema.ToProto();
  for (int i = 0; i < proto.edge_types_size(); ++i) {
    const auto& et = proto.edge_types(i);
    for (int j = 0; j < et.properties_size(); ++j) {
      prop_names.insert(et.properties(j).name());
    }
  }

  for (const auto& name : prop_names) {
    cs.add_field(std::make_shared<FieldSchema>(name, DataType::STRING,
                                               /*nullable=*/true));
  }

  // System fields for mutation tracking.
  cs.add_field(std::make_shared<FieldSchema>("_version", DataType::UINT64,
                                             /*nullable=*/true));
  cs.add_field(std::make_shared<FieldSchema>("_updated_at", DataType::UINT64,
                                             /*nullable=*/true));

  // zvec requires at least one vector field per collection.
  cs.add_field(std::make_shared<FieldSchema>(
      kPlaceholderVectorField, DataType::VECTOR_FP32, kPlaceholderVectorDim,
      /*nullable=*/false,
      std::make_shared<FlatIndexParams>(MetricType::IP)));

  return cs;
}

// ---------------------------------------------------------------------------
// Create / Open
// ---------------------------------------------------------------------------

std::unique_ptr<ZvecStorage> ZvecStorage::Create(const std::string& path,
                                                 const GraphSchema& schema) {
  auto nodes_schema = BuildNodesSchema(schema);
  auto edges_schema = BuildEdgesSchema(schema);

  CollectionOptions opts{/*read_only=*/false, /*enable_mmap=*/true};

  std::string nodes_path = path + "/nodes";
  std::string edges_path = path + "/edges";

  auto nr = Collection::CreateAndOpen(nodes_path, nodes_schema, opts);
  if (!nr.has_value()) {
    LOG_ERROR("ZvecStorage::Create: failed to create nodes collection at %s: %s",
              nodes_path.c_str(), nr.error().message().c_str());
    return nullptr;
  }

  auto er = Collection::CreateAndOpen(edges_path, edges_schema, opts);
  if (!er.has_value()) {
    LOG_ERROR("ZvecStorage::Create: failed to create edges collection at %s: %s",
              edges_path.c_str(), er.error().message().c_str());
    // Clean up the nodes collection that was already created.
    nr.value()->Destroy();
    return nullptr;
  }

  auto storage = std::unique_ptr<ZvecStorage>(new ZvecStorage());
  storage->path_ = path;
  storage->nodes_collection_ = std::move(nr.value());
  storage->edges_collection_ = std::move(er.value());
  return storage;
}

std::unique_ptr<ZvecStorage> ZvecStorage::Open(const std::string& path) {
  CollectionOptions opts{/*read_only=*/false, /*enable_mmap=*/true};

  std::string nodes_path = path + "/nodes";
  std::string edges_path = path + "/edges";

  auto nr = Collection::Open(nodes_path, opts);
  if (!nr.has_value()) {
    return nullptr;
  }

  auto er = Collection::Open(edges_path, opts);
  if (!er.has_value()) {
    return nullptr;
  }

  auto storage = std::unique_ptr<ZvecStorage>(new ZvecStorage());
  storage->path_ = path;
  storage->nodes_collection_ = std::move(nr.value());
  storage->edges_collection_ = std::move(er.value());
  return storage;
}

// ---------------------------------------------------------------------------
// Doc <-> GraphNode conversion
// ---------------------------------------------------------------------------

Doc ZvecStorage::NodeToDoc(const GraphNode& node) {
  Doc doc;
  doc.set_pk(node.id);
  doc.set<std::string>("node_type", node.node_type);

  for (const auto& [key, value] : node.properties) {
    doc.set<std::string>(key, value);
  }

  // Serialize vectors as comma-separated float strings under a prefixed key.
  for (const auto& [name, vec] : node.vectors) {
    std::string serialized;
    for (size_t i = 0; i < vec.size(); ++i) {
      if (i > 0) serialized += ',';
      serialized += std::to_string(vec[i]);
    }
    doc.set<std::string>(kVectorPropertyPrefix + name, serialized);
  }

  // System fields.
  doc.set<uint64_t>("_version", node.version);
  doc.set<uint64_t>("_updated_at", node.updated_at);

  // Adjacency lists.
  doc.set<std::vector<std::string>>("_neighbor_ids", node.neighbor_ids);
  doc.set<std::vector<std::string>>("_neighbor_edge_ids",
                                    node.neighbor_edge_ids);

  // Always provide the placeholder vector.
  doc.set<std::vector<float>>(kPlaceholderVectorField,
                              std::vector<float>(kPlaceholderVectorDim, 0.0f));

  return doc;
}

GraphNode ZvecStorage::DocToNode(const Doc& doc) {
  GraphNode node;
  node.id = doc.pk();

  auto nt = doc.get<std::string>("node_type");
  if (nt.has_value()) {
    node.node_type = nt.value();
  }

  // System fields.
  auto ver = doc.get<uint64_t>("_version");
  if (ver.has_value()) node.version = ver.value();

  auto upd = doc.get<uint64_t>("_updated_at");
  if (upd.has_value()) node.updated_at = upd.value();

  // Adjacency lists.
  auto nids = doc.get<std::vector<std::string>>("_neighbor_ids");
  if (nids.has_value()) node.neighbor_ids = nids.value();

  auto neids = doc.get<std::vector<std::string>>("_neighbor_edge_ids");
  if (neids.has_value()) node.neighbor_edge_ids = neids.value();

  // Internal system field names to exclude from user properties.
  static const std::unordered_set<std::string> kNodeSystemFields = {
      "node_type",          kPlaceholderVectorField, "_version",
      "_updated_at",        "_neighbor_ids",         "_neighbor_edge_ids"};

  // Extract all string fields as properties (excluding system fields).
  for (const auto& field_name : doc.field_names()) {
    if (kNodeSystemFields.count(field_name) > 0) continue;

    auto str_val = doc.get<std::string>(field_name);
    if (!str_val.has_value()) continue;

    // Check if this is a serialized vector (prefixed with "_vec_").
    if (field_name.rfind(kVectorPropertyPrefix, 0) == 0) {
      std::string vec_name = field_name.substr(kVectorPropertyPrefix.size());
      std::vector<float> vec;
      std::istringstream iss(str_val.value());
      std::string token;
      while (std::getline(iss, token, ',')) {
        vec.push_back(std::stof(token));
      }
      node.vectors[vec_name] = std::move(vec);
    } else {
      node.properties[field_name] = str_val.value();
    }
  }

  return node;
}

// ---------------------------------------------------------------------------
// Doc <-> GraphEdge conversion
// ---------------------------------------------------------------------------

Doc ZvecStorage::EdgeToDoc(const GraphEdge& edge) {
  Doc doc;
  doc.set_pk(edge.id);
  doc.set<std::string>("source_id", edge.source_id);
  doc.set<std::string>("target_id", edge.target_id);
  doc.set<std::string>("edge_type", edge.edge_type);
  doc.set<bool>("directed", edge.directed);

  for (const auto& [key, value] : edge.properties) {
    doc.set<std::string>(key, value);
  }

  // System fields.
  doc.set<uint64_t>("_version", edge.version);
  doc.set<uint64_t>("_updated_at", edge.updated_at);

  // Set the placeholder vector required by zvec.
  doc.set<std::vector<float>>(kPlaceholderVectorField,
                              std::vector<float>(kPlaceholderVectorDim, 0.0f));

  return doc;
}

GraphEdge ZvecStorage::DocToEdge(const Doc& doc) {
  GraphEdge edge;
  edge.id = doc.pk();

  auto src = doc.get<std::string>("source_id");
  if (src.has_value()) edge.source_id = src.value();

  auto tgt = doc.get<std::string>("target_id");
  if (tgt.has_value()) edge.target_id = tgt.value();

  auto et = doc.get<std::string>("edge_type");
  if (et.has_value()) edge.edge_type = et.value();

  auto dir = doc.get<bool>("directed");
  if (dir.has_value()) edge.directed = dir.value();

  // System fields.
  auto ver = doc.get<uint64_t>("_version");
  if (ver.has_value()) edge.version = ver.value();

  auto upd = doc.get<uint64_t>("_updated_at");
  if (upd.has_value()) edge.updated_at = upd.value();

  // Extract remaining string fields as properties.
  static const std::unordered_set<std::string> kEdgeSystemFields = {
      "source_id",  "target_id", "edge_type", "directed",
      "_version",   "_updated_at", kPlaceholderVectorField};

  for (const auto& field_name : doc.field_names()) {
    if (kEdgeSystemFields.count(field_name) > 0) continue;

    auto str_val = doc.get<std::string>(field_name);
    if (str_val.has_value()) {
      edge.properties[field_name] = str_val.value();
    }
  }

  return edge;
}

// ---------------------------------------------------------------------------
// Node CRUD
// ---------------------------------------------------------------------------

Status ZvecStorage::UpsertNodes(const std::vector<GraphNode>& nodes) {
  if (nodes.empty()) return Status::OK();

  std::vector<Doc> docs;
  docs.reserve(nodes.size());
  for (const auto& node : nodes) {
    docs.push_back(NodeToDoc(node));
  }

  auto result = nodes_collection_->Upsert(docs);
  if (!result.has_value()) {
    return Status::InternalError("UpsertNodes failed: ",
                                 result.error().message());
  }

  // Check individual write results.
  for (size_t i = 0; i < result.value().size(); ++i) {
    if (!result.value()[i].ok()) {
      return Status::InternalError("UpsertNodes failed for node ",
                                   nodes[i].id, ": ",
                                   result.value()[i].message());
    }
  }

  return Status::OK();
}

Status ZvecStorage::DeleteNodes(const std::vector<std::string>& node_ids) {
  if (node_ids.empty()) return Status::OK();

  auto result = nodes_collection_->Delete(node_ids);
  if (!result.has_value()) {
    return Status::InternalError("DeleteNodes failed: ",
                                 result.error().message());
  }

  // Individual delete errors for non-existent keys are acceptable.
  return Status::OK();
}

Result<std::vector<GraphNode>> ZvecStorage::FetchNodes(
    const std::vector<std::string>& node_ids) {
  if (node_ids.empty()) return std::vector<GraphNode>{};

  auto result = nodes_collection_->Fetch(node_ids);
  if (!result.has_value()) {
    return tl::make_unexpected(
        Status::InternalError("FetchNodes failed: ",
                              result.error().message()));
  }

  std::vector<GraphNode> nodes;
  for (const auto& [pk, doc_ptr] : result.value()) {
    if (doc_ptr != nullptr) {
      nodes.push_back(DocToNode(*doc_ptr));
    }
  }

  return nodes;
}

Result<std::vector<GraphNode>> ZvecStorage::FilterNodes(
    const std::string& filter_expr, int limit) {
  // Use VectorQuery with filter only (no vector search).
  VectorQuery query;
  query.topk_ = limit;
  query.filter_ = filter_expr;
  query.include_vector_ = false;

  auto result = nodes_collection_->Query(query);
  if (!result.has_value()) {
    return tl::make_unexpected(
        Status::InternalError("FilterNodes failed: ",
                              result.error().message()));
  }

  std::vector<GraphNode> nodes;
  for (const auto& doc_ptr : result.value()) {
    if (doc_ptr != nullptr) {
      nodes.push_back(DocToNode(*doc_ptr));
    }
  }

  return nodes;
}

Result<std::vector<GraphNode>> ZvecStorage::QueryNodes(
    const std::string& vector_field,
    const std::vector<float>& query_vector, int topk,
    const std::string& filter_expr) {
  VectorQuery query;
  query.topk_ = topk;
  query.field_name_ = vector_field;
  query.include_vector_ = true;

  if (!filter_expr.empty()) {
    query.filter_ = filter_expr;
  }

  // Serialize query_vector into a byte string for VectorQuery.
  query.query_vector_.assign(
      reinterpret_cast<const char*>(query_vector.data()),
      query_vector.size() * sizeof(float));

  auto result = nodes_collection_->Query(query);
  if (!result.has_value()) {
    return tl::make_unexpected(
        Status::InternalError("QueryNodes failed: ",
                              result.error().message()));
  }

  std::vector<GraphNode> nodes;
  for (const auto& doc_ptr : result.value()) {
    if (doc_ptr != nullptr) {
      nodes.push_back(DocToNode(*doc_ptr));
    }
  }

  return nodes;
}

// ---------------------------------------------------------------------------
// Edge CRUD
// ---------------------------------------------------------------------------

Status ZvecStorage::UpsertEdges(const std::vector<GraphEdge>& edges) {
  if (edges.empty()) return Status::OK();

  std::vector<Doc> docs;
  docs.reserve(edges.size());
  for (const auto& edge : edges) {
    docs.push_back(EdgeToDoc(edge));
  }

  auto result = edges_collection_->Upsert(docs);
  if (!result.has_value()) {
    return Status::InternalError("UpsertEdges failed: ",
                                 result.error().message());
  }

  for (size_t i = 0; i < result.value().size(); ++i) {
    if (!result.value()[i].ok()) {
      return Status::InternalError("UpsertEdges failed for edge ",
                                   edges[i].id, ": ",
                                   result.value()[i].message());
    }
  }

  return Status::OK();
}

Status ZvecStorage::DeleteEdges(const std::vector<std::string>& edge_ids) {
  if (edge_ids.empty()) return Status::OK();

  auto result = edges_collection_->Delete(edge_ids);
  if (!result.has_value()) {
    return Status::InternalError("DeleteEdges failed: ",
                                 result.error().message());
  }

  return Status::OK();
}

Result<std::vector<GraphEdge>> ZvecStorage::FetchEdges(
    const std::vector<std::string>& edge_ids) {
  if (edge_ids.empty()) return std::vector<GraphEdge>{};

  auto result = edges_collection_->Fetch(edge_ids);
  if (!result.has_value()) {
    return tl::make_unexpected(
        Status::InternalError("FetchEdges failed: ",
                              result.error().message()));
  }

  std::vector<GraphEdge> edges;
  for (const auto& [pk, doc_ptr] : result.value()) {
    if (doc_ptr != nullptr) {
      edges.push_back(DocToEdge(*doc_ptr));
    }
  }

  return edges;
}

Result<std::vector<GraphEdge>> ZvecStorage::FilterEdges(
    const std::string& filter_expr, int limit) {
  VectorQuery query;
  query.topk_ = limit;
  query.filter_ = filter_expr;
  query.include_vector_ = false;

  auto result = edges_collection_->Query(query);
  if (!result.has_value()) {
    return tl::make_unexpected(
        Status::InternalError("FilterEdges failed: ",
                              result.error().message()));
  }

  std::vector<GraphEdge> edges;
  for (const auto& doc_ptr : result.value()) {
    if (doc_ptr != nullptr) {
      edges.push_back(DocToEdge(*doc_ptr));
    }
  }

  return edges;
}

// ---------------------------------------------------------------------------
// Batch operations
// ---------------------------------------------------------------------------

Status ZvecStorage::AtomicBatch(const std::vector<Mutation>& mutations) {
  // NOTE: This implementation executes mutations sequentially. It is NOT
  // truly atomic — a failure partway through will leave the store in an
  // inconsistent state. True atomicity requires WriteBatch-level integration
  // with the underlying RocksDB engine (future work).
  for (const auto& m : mutations) {
    Status s;
    switch (m.type) {
      case MutationType::UPSERT_NODE:
        s = UpsertNodes({m.node});
        break;
      case MutationType::DELETE_NODE:
        s = DeleteNodes({m.node.id});
        break;
      case MutationType::UPSERT_EDGE:
        s = UpsertEdges({m.edge});
        break;
      case MutationType::DELETE_EDGE:
        s = DeleteEdges({m.edge.id});
        break;
    }
    if (!s.ok()) return s;
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// Index operations
// ---------------------------------------------------------------------------

Status ZvecStorage::CreateIndex(const std::string& collection_name,
                                const std::string& field_name) {
  Collection::Ptr col;
  if (collection_name == "nodes") {
    col = nodes_collection_;
  } else if (collection_name == "edges") {
    col = edges_collection_;
  } else {
    return Status::InvalidArgument("Unknown collection: ", collection_name);
  }

  auto index_params = std::make_shared<InvertIndexParams>(/*enable_optimize=*/true);
  return col->CreateIndex(field_name, index_params);
}

Status ZvecStorage::DropIndex(const std::string& collection_name,
                              const std::string& field_name) {
  Collection::Ptr col;
  if (collection_name == "nodes") {
    col = nodes_collection_;
  } else if (collection_name == "edges") {
    col = edges_collection_;
  } else {
    return Status::InvalidArgument("Unknown collection: ", collection_name);
  }

  return col->DropIndex(field_name);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

Status ZvecStorage::Flush() {
  auto s1 = nodes_collection_->Flush();
  if (!s1.ok()) return s1;
  return edges_collection_->Flush();
}

Status ZvecStorage::Destroy() {
  auto s1 = nodes_collection_->Destroy();
  auto s2 = edges_collection_->Destroy();
  // Try to remove the parent directory as well.
  ailego::FileHelper::RemoveDirectory(path_.c_str());
  if (!s1.ok()) return s1;
  return s2;
}

}  // namespace graph
}  // namespace zvec
