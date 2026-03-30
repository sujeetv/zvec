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

#include <string>
#include <unordered_map>
#include <vector>

namespace zvec {
namespace graph {

struct GraphNode {
  std::string id;
  std::string node_type;
  std::unordered_map<std::string, std::string> properties;
  // vectors stored as named float arrays
  std::unordered_map<std::string, std::vector<float>> vectors;
  // adjacency
  std::vector<std::string> neighbor_ids;
  std::vector<std::string> neighbor_edge_ids;
  // system fields
  uint64_t version = 0;
  uint64_t updated_at = 0;
  bool deleted = false;
};

}  // namespace graph
}  // namespace zvec
