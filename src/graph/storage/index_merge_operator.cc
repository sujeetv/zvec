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

#include "graph/storage/index_merge_operator.h"

#include <algorithm>
#include <unordered_set>

#include "graph/proto/graph.pb.h"

namespace zvec {
namespace graph {

std::string IndexMergeOperator::EncodeAdd(const std::string& id) {
  std::string operand;
  operand.reserve(1 + id.size());
  operand.push_back(static_cast<char>(kOpAdd));
  operand.append(id);
  return operand;
}

std::string IndexMergeOperator::EncodeRemove(const std::string& id) {
  std::string operand;
  operand.reserve(1 + id.size());
  operand.push_back(static_cast<char>(kOpRemove));
  operand.append(id);
  return operand;
}

bool IndexMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  // Start from existing value (if any)
  proto::IndexEntry entry;
  if (merge_in.existing_value) {
    if (!entry.ParseFromArray(merge_in.existing_value->data(),
                              static_cast<int>(merge_in.existing_value->size()))) {
      return false;
    }
  }

  // Build a set from existing IDs for efficient add/remove
  std::unordered_set<std::string> id_set(entry.ids().begin(), entry.ids().end());

  // Apply each operand in order
  for (const auto& operand : merge_in.operand_list) {
    if (operand.size() < 2) continue;
    uint8_t op = static_cast<uint8_t>(operand[0]);
    std::string id = operand.ToString().substr(1);

    if (op == kOpAdd) {
      id_set.insert(std::move(id));
    } else if (op == kOpRemove) {
      id_set.erase(id);
    }
  }

  // Serialize back
  entry.clear_ids();
  for (const auto& id : id_set) {
    entry.add_ids(id);
  }

  if (!entry.SerializeToString(&merge_out->new_value)) {
    return false;
  }

  return true;
}

bool IndexMergeOperator::PartialMerge(const rocksdb::Slice& /*key*/,
                                      const rocksdb::Slice& /*left_operand*/,
                                      const rocksdb::Slice& /*right_operand*/,
                                      std::string* /*new_value*/,
                                      rocksdb::Logger* /*logger*/) const {
  // Don't combine partial merges — let FullMerge handle everything.
  // This is safe and correct; RocksDB will accumulate operands.
  return false;
}

}  // namespace graph
}  // namespace zvec
