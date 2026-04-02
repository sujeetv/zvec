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

#include "graph/mutation_engine.h"

#include <algorithm>
#include <chrono>

namespace zvec {
namespace graph {

MutationEngine::MutationEngine(const GraphSchema* schema,
                               StorageInterface* storage)
    : schema_(schema), storage_(storage) {}

std::string MutationEngine::MakeEdgeId(const std::string& source,
                                       const std::string& edge_type,
                                       const std::string& target) {
  return source + "--" + edge_type + "--" + target;
}

uint64_t MutationEngine::Now() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

Status MutationEngine::AddNode(const GraphNode& node) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Validate node type and required properties against schema
  auto status = schema_->ValidateNode(node.node_type, node.properties);
  if (!status.ok()) {
    return status;
  }

  // Set system fields
  GraphNode n = node;
  n.version = 1;
  n.updated_at = Now();

  return storage_->UpsertNodes({n});
}

Status MutationEngine::RemoveNode(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Fetch the node
  auto result = storage_->FetchNodes({node_id});
  if (!result.has_value() || result.value().empty()) {
    // Node doesn't exist — treat as success (idempotent)
    return Status::OK();
  }

  const GraphNode& node = result.value()[0];

  // Cascade-delete all connected edges.
  // Copy edge IDs since RemoveEdge modifies adjacency lists.
  std::vector<std::string> edge_ids = node.neighbor_edge_ids;
  for (const auto& edge_id : edge_ids) {
    // Inline edge removal (without re-locking)
    auto edge_result = storage_->FetchEdges({edge_id});
    if (!edge_result.has_value() || edge_result.value().empty()) {
      continue;
    }
    const GraphEdge& edge = edge_result.value()[0];

    // Determine the other node
    std::string other_id =
        (edge.source_id == node_id) ? edge.target_id : edge.source_id;

    // Fetch the other node and clean its adjacency
    auto other_result = storage_->FetchNodes({other_id});
    if (other_result.has_value() && !other_result.value().empty()) {
      GraphNode other = other_result.value()[0];

      // Remove this edge from the other node's neighbor_edge_ids
      other.neighbor_edge_ids.erase(
          std::remove(other.neighbor_edge_ids.begin(),
                      other.neighbor_edge_ids.end(), edge_id),
          other.neighbor_edge_ids.end());

      // Check if there are still other edges connecting to node_id
      bool still_connected = false;
      if (!other.neighbor_edge_ids.empty()) {
        auto batch_edges =
            storage_->FetchEdges(other.neighbor_edge_ids);
        if (batch_edges.has_value()) {
          for (const auto& e : batch_edges.value()) {
            if (e.source_id == node_id || e.target_id == node_id) {
              still_connected = true;
              break;
            }
          }
        }
      }
      if (!still_connected) {
        other.neighbor_ids.erase(
            std::remove(other.neighbor_ids.begin(), other.neighbor_ids.end(),
                        node_id),
            other.neighbor_ids.end());
      }

      other.version++;
      other.updated_at = Now();

      std::vector<Mutation> batch;
      batch.push_back(Mutation::DeleteEdge(edge));
      batch.push_back(Mutation::UpsertNode(other));
      auto s = storage_->AtomicBatch(batch);
      if (!s.ok()) {
        return s;
      }
    } else {
      // Other node missing — just delete the edge
      storage_->DeleteEdges({edge_id});
    }
  }

  // Delete the node itself
  return storage_->DeleteNodes({node_id});
}

Status MutationEngine::UpdateNode(
    const std::string& node_id,
    const std::unordered_map<std::string, std::string>& properties) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto result = storage_->FetchNodes({node_id});
  if (!result.has_value() || result.value().empty()) {
    return Status::NotFound("Node '", node_id, "' not found");
  }

  GraphNode node = result.value()[0];

  // Merge new properties into existing
  for (const auto& [key, value] : properties) {
    node.properties[key] = value;
  }

  node.version++;
  node.updated_at = Now();

  return storage_->UpsertNodes({node});
}

Status MutationEngine::AddEdge(
    const std::string& source_id, const std::string& target_id,
    const std::string& edge_type,
    const std::unordered_map<std::string, std::string>& properties) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Fetch source and target nodes — both must exist
  auto src_result = storage_->FetchNodes({source_id});
  if (!src_result.has_value() || src_result.value().empty()) {
    return Status::NotFound("Source node '", source_id, "' not found");
  }

  auto tgt_result = storage_->FetchNodes({target_id});
  if (!tgt_result.has_value() || tgt_result.value().empty()) {
    return Status::NotFound("Target node '", target_id, "' not found");
  }

  GraphNode source = src_result.value()[0];
  GraphNode target = tgt_result.value()[0];

  // Validate edge against schema (type + constraints)
  auto status = schema_->ValidateEdge(edge_type, source.node_type,
                                      target.node_type, source_id, target_id);
  if (!status.ok()) {
    return status;
  }

  // Generate deterministic edge ID
  std::string edge_id = MakeEdgeId(source_id, edge_type, target_id);

  // Idempotency: if edge already exists in source's adjacency, return OK
  if (std::find(source.neighbor_edge_ids.begin(),
                source.neighbor_edge_ids.end(),
                edge_id) != source.neighbor_edge_ids.end()) {
    return Status::OK();
  }

  // Build edge document
  GraphEdge edge;
  edge.id = edge_id;
  edge.source_id = source_id;
  edge.target_id = target_id;
  edge.edge_type = edge_type;
  edge.properties = properties;
  edge.version = 1;
  edge.updated_at = Now();

  // Check if the edge type is directed
  const auto* edge_type_def = schema_->GetEdgeType(edge_type);
  if (edge_type_def) {
    edge.directed = edge_type_def->directed();
  }

  // Update source node adjacency
  source.neighbor_ids.push_back(target_id);
  source.neighbor_edge_ids.push_back(edge_id);
  source.version++;
  source.updated_at = Now();

  // Update target node adjacency
  target.neighbor_ids.push_back(source_id);
  target.neighbor_edge_ids.push_back(edge_id);
  target.version++;
  target.updated_at = Now();

  // Atomic batch: edge upsert + both node upserts
  std::vector<Mutation> batch;
  batch.push_back(Mutation::UpsertEdge(edge));
  batch.push_back(Mutation::UpsertNode(source));
  batch.push_back(Mutation::UpsertNode(target));

  return storage_->AtomicBatch(batch);
}

Status MutationEngine::RemoveEdge(const std::string& edge_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Fetch the edge
  auto edge_result = storage_->FetchEdges({edge_id});
  if (!edge_result.has_value() || edge_result.value().empty()) {
    return Status::NotFound("Edge '", edge_id, "' not found");
  }

  const GraphEdge& edge = edge_result.value()[0];

  // Fetch both endpoint nodes
  auto src_result = storage_->FetchNodes({edge.source_id});
  auto tgt_result = storage_->FetchNodes({edge.target_id});

  std::vector<Mutation> batch;
  batch.push_back(Mutation::DeleteEdge(edge));

  // Clean up source node adjacency
  if (src_result.has_value() && !src_result.value().empty()) {
    GraphNode source = src_result.value()[0];
    source.neighbor_edge_ids.erase(
        std::remove(source.neighbor_edge_ids.begin(),
                    source.neighbor_edge_ids.end(), edge_id),
        source.neighbor_edge_ids.end());

    // Only remove neighbor_id if no other edges connect to the same target
    bool still_connected = false;
    if (!source.neighbor_edge_ids.empty()) {
      auto batch_edges =
          storage_->FetchEdges(source.neighbor_edge_ids);
      if (batch_edges.has_value()) {
        for (const auto& e : batch_edges.value()) {
          if (e.source_id == edge.target_id ||
              e.target_id == edge.target_id) {
            still_connected = true;
            break;
          }
        }
      }
    }
    if (!still_connected) {
      source.neighbor_ids.erase(
          std::remove(source.neighbor_ids.begin(), source.neighbor_ids.end(),
                      edge.target_id),
          source.neighbor_ids.end());
    }

    source.version++;
    source.updated_at = Now();
    batch.push_back(Mutation::UpsertNode(source));
  }

  // Clean up target node adjacency
  if (tgt_result.has_value() && !tgt_result.value().empty()) {
    GraphNode target = tgt_result.value()[0];
    target.neighbor_edge_ids.erase(
        std::remove(target.neighbor_edge_ids.begin(),
                    target.neighbor_edge_ids.end(), edge_id),
        target.neighbor_edge_ids.end());

    // Only remove neighbor_id if no other edges connect to the same source
    bool still_connected = false;
    if (!target.neighbor_edge_ids.empty()) {
      auto batch_edges =
          storage_->FetchEdges(target.neighbor_edge_ids);
      if (batch_edges.has_value()) {
        for (const auto& e : batch_edges.value()) {
          if (e.source_id == edge.source_id ||
              e.target_id == edge.source_id) {
            still_connected = true;
            break;
          }
        }
      }
    }
    if (!still_connected) {
      target.neighbor_ids.erase(
          std::remove(target.neighbor_ids.begin(), target.neighbor_ids.end(),
                      edge.source_id),
          target.neighbor_ids.end());
    }

    target.version++;
    target.updated_at = Now();
    batch.push_back(Mutation::UpsertNode(target));
  }

  return storage_->AtomicBatch(batch);
}

Status MutationEngine::UpdateEdge(
    const std::string& edge_id,
    const std::unordered_map<std::string, std::string>& properties) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto result = storage_->FetchEdges({edge_id});
  if (!result.has_value() || result.value().empty()) {
    return Status::NotFound("Edge '", edge_id, "' not found");
  }

  GraphEdge edge = result.value()[0];

  // Merge new properties into existing
  for (const auto& [key, value] : properties) {
    edge.properties[key] = value;
  }

  edge.version++;
  edge.updated_at = Now();

  return storage_->UpsertEdges({edge});
}

}  // namespace graph
}  // namespace zvec
