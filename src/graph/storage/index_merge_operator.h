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

#include <rocksdb/merge_operator.h>

namespace zvec {
namespace graph {

//! Merge operand format: [1 byte op_type][entity_id string]
//!   op_type 0x01 = ADD id to the index entry
//!   op_type 0x02 = REMOVE id from the index entry
//!
//! The underlying value is a serialized IndexEntry protobuf.
class IndexMergeOperator : public rocksdb::MergeOperator {
 public:
  static constexpr uint8_t kOpAdd = 0x01;
  static constexpr uint8_t kOpRemove = 0x02;

  //! Encode an ADD operand.
  static std::string EncodeAdd(const std::string& id);

  //! Encode a REMOVE operand.
  static std::string EncodeRemove(const std::string& id);

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

  bool PartialMerge(const rocksdb::Slice& key,
                    const rocksdb::Slice& left_operand,
                    const rocksdb::Slice& right_operand,
                    std::string* new_value,
                    rocksdb::Logger* logger) const override;

  const char* Name() const override { return "IndexMergeOperator"; }
};

}  // namespace graph
}  // namespace zvec
