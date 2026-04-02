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

#include "graph/traversal.h"

#include <algorithm>
#include <queue>
#include <unordered_map>
#include <unordered_set>

namespace zvec {
namespace graph {

TraversalEngine::TraversalEngine(StorageInterface* storage)
    : storage_(storage) {}

bool TraversalEngine::ParseFilter(const std::string& filter,
                                  std::string& field, std::string& value) {
  // Parse "field = 'value'" format
  auto eq_pos = filter.find('=');
  if (eq_pos == std::string::npos) return false;

  // Extract field name (trim whitespace)
  field = filter.substr(0, eq_pos);
  while (!field.empty() && field.back() == ' ') field.pop_back();
  while (!field.empty() && field.front() == ' ')
    field = field.substr(1);

  // Extract value (strip surrounding quotes and whitespace)
  value = filter.substr(eq_pos + 1);
  while (!value.empty() && value.front() == ' ')
    value = value.substr(1);
  while (!value.empty() && value.back() == ' ') value.pop_back();

  // Remove surrounding single quotes
  if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
    value = value.substr(1, value.size() - 2);
  }

  return !field.empty() && !value.empty();
}

bool TraversalEngine::EdgeMatchesFilter(const GraphEdge& edge,
                                        const std::string& field,
                                        const std::string& value) {
  if (field == "edge_type") return edge.edge_type == value;
  if (field == "source_id") return edge.source_id == value;
  if (field == "target_id") return edge.target_id == value;
  if (field == "id") return edge.id == value;
  // Check properties
  auto it = edge.properties.find(field);
  return it != edge.properties.end() && it->second == value;
}

bool TraversalEngine::NodeMatchesFilter(const GraphNode& node,
                                        const std::string& field,
                                        const std::string& value) {
  if (field == "node_type") return node.node_type == value;
  if (field == "id") return node.id == value;
  // Check properties
  auto it = node.properties.find(field);
  return it != node.properties.end() && it->second == value;
}

Subgraph TraversalEngine::Traverse(const TraversalParams& params) const {
  Subgraph result;

  // 1. Empty start_ids → empty result
  if (params.start_ids.empty()) return result;

  // Parse filters once
  std::string edge_field, edge_value;
  bool has_edge_filter =
      !params.edge_filter.empty() &&
      ParseFilter(params.edge_filter, edge_field, edge_value);

  std::string node_field, node_value;
  bool has_node_filter =
      !params.node_filter.empty() &&
      ParseFilter(params.node_filter, node_field, node_value);

  std::unordered_set<std::string> visited;
  std::unordered_set<std::string> collected_edge_ids;
  std::unordered_map<std::string, GraphNode> node_cache;

  // 2. Fetch seed nodes (lite — skip vector deserialization)
  auto seed_result = storage_->FetchNodesLite(params.start_ids);
  if (!seed_result.has_value()) return result;

  std::vector<std::string> frontier;
  frontier.reserve(params.start_ids.size());
  for (auto& node : seed_result.value()) {
    if (visited.count(node.id)) continue;
    visited.insert(node.id);
    frontier.push_back(node.id);
    node_cache[node.id] = node;
    result.nodes.push_back(std::move(node));
  }

  // Check max_nodes budget after seeds
  if (params.max_nodes > 0 &&
      static_cast<int>(result.nodes.size()) >= params.max_nodes) {
    // Trim to budget
    if (static_cast<int>(result.nodes.size()) > params.max_nodes) {
      result.nodes.resize(params.max_nodes);
      result.truncated = true;
    }
    return result;
  }

  // 3. If max_depth == 0, return just seeds
  if (params.max_depth == 0) return result;

  // 4. BFS hops
  for (int hop = 1; hop <= params.max_depth; ++hop) {
    if (frontier.empty()) break;

    // Resolve frontier nodes from cache, fetch any cache misses.
    std::vector<GraphNode> frontier_nodes;
    frontier_nodes.reserve(frontier.size());
    {
      std::vector<std::string> missing_ids;
      for (const auto& id : frontier) {
        auto it = node_cache.find(id);
        if (it != node_cache.end()) {
          frontier_nodes.push_back(it->second);
        } else {
          missing_ids.push_back(id);
        }
      }
      if (!missing_ids.empty()) {
        auto fetched = storage_->FetchNodesLite(missing_ids);
        if (fetched.has_value()) {
          for (auto& n : fetched.value()) {
            node_cache[n.id] = n;
            frontier_nodes.push_back(std::move(n));
          }
        }
      }
    }

    // Collect candidate (neighbor_id, edge_id) pairs
    std::vector<std::string> candidate_edge_ids;
    candidate_edge_ids.reserve(frontier_nodes.size() * 8);
    for (const auto& node : frontier_nodes) {
      for (size_t i = 0; i < node.neighbor_ids.size(); ++i) {
        const auto& neighbor_id = node.neighbor_ids[i];
        if (visited.count(neighbor_id)) continue;
        if (i < node.neighbor_edge_ids.size()) {
          const auto& edge_id = node.neighbor_edge_ids[i];
          if (!collected_edge_ids.count(edge_id)) {
            candidate_edge_ids.push_back(edge_id);
          }
        }
      }
    }

    if (candidate_edge_ids.empty()) break;

    // Batch-fetch edges
    auto edges_result = storage_->FetchEdges(candidate_edge_ids);
    if (!edges_result.has_value()) break;

    // Apply edge filter
    std::vector<GraphEdge> passing_edges;
    passing_edges.reserve(edges_result.value().size());
    std::vector<std::string> target_node_ids;
    target_node_ids.reserve(edges_result.value().size());

    for (auto& edge : edges_result.value()) {
      if (has_edge_filter &&
          !EdgeMatchesFilter(edge, edge_field, edge_value)) {
        continue;
      }

      // Determine the neighbor node: could be source or target
      // depending on direction
      std::string neighbor_id;
      if (!visited.count(edge.target_id)) {
        neighbor_id = edge.target_id;
      } else if (!visited.count(edge.source_id)) {
        neighbor_id = edge.source_id;
      } else {
        // Both visited — still record the edge if not collected
        if (!collected_edge_ids.count(edge.id)) {
          collected_edge_ids.insert(edge.id);
          result.edges.push_back(std::move(edge));
        }
        continue;
      }

      if (!collected_edge_ids.count(edge.id)) {
        target_node_ids.push_back(neighbor_id);
        collected_edge_ids.insert(edge.id);
        passing_edges.push_back(std::move(edge));
      }
    }

    if (target_node_ids.empty()) {
      // Still add any edges that were collected above
      break;
    }

    // Deduplicate target node IDs
    std::unordered_set<std::string> unique_targets(target_node_ids.begin(),
                                                   target_node_ids.end());
    target_node_ids.assign(unique_targets.begin(), unique_targets.end());

    // Batch-fetch target nodes (lite — skip vector deserialization)
    auto nodes_result = storage_->FetchNodesLite(target_node_ids);
    if (!nodes_result.has_value()) {
      // Still add the edges
      for (auto& e : passing_edges) result.edges.push_back(std::move(e));
      break;
    }

    // Apply node filter and budget
    std::vector<std::string> new_frontier;
    new_frontier.reserve(nodes_result.value().size());
    std::vector<GraphEdge> accepted_edges;

    for (auto& node : nodes_result.value()) {
      if (visited.count(node.id)) continue;

      // Apply node filter (seed nodes are exempt, but these aren't seeds)
      if (has_node_filter &&
          !NodeMatchesFilter(node, node_field, node_value)) {
        continue;
      }

      // Check budget
      if (params.max_nodes > 0 &&
          static_cast<int>(result.nodes.size()) + 1 > params.max_nodes) {
        result.truncated = true;
        break;
      }

      visited.insert(node.id);
      new_frontier.push_back(node.id);
      node_cache[node.id] = node;
      result.nodes.push_back(std::move(node));
    }

    // Add edges whose target nodes were accepted
    std::unordered_set<std::string> accepted_node_ids(new_frontier.begin(),
                                                      new_frontier.end());
    for (auto& edge : passing_edges) {
      std::string neighbor_id;
      if (accepted_node_ids.count(edge.target_id) ||
          visited.count(edge.target_id)) {
        // Edge connects to an accepted or previously visited node
        result.edges.push_back(std::move(edge));
      } else if (accepted_node_ids.count(edge.source_id) ||
                 visited.count(edge.source_id)) {
        result.edges.push_back(std::move(edge));
      }
    }

    if (result.truncated) break;

    frontier = std::move(new_frontier);
  }

  return result;
}

}  // namespace graph
}  // namespace zvec
