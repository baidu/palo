// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include "vec/common/arena.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/core/block.h"

namespace doris::vectorized {
/// Reference to the row in block.
struct RowRef {
    using SizeT = uint32_t; /// Do not use size_t cause of memory economy

    const Block* block = nullptr;
    SizeT row_num = 0;

    RowRef() {}
    RowRef(const Block* block_, size_t row_num_) : block(block_), row_num(row_num_) {}
};

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
struct RowRefList : RowRef {
    /// Portion of RowRefs, 16 * (MAX_SIZE + 1) bytes sized.
    struct Batch {
        static constexpr size_t MAX_SIZE = 7; /// Adequate values are 3, 7, 15, 31.

        SizeT size = 0; /// It's smaller than size_t but keeps align in Arena.
        Batch* next;
        RowRef row_refs[MAX_SIZE];

        Batch(Batch* parent) : next(parent) {}

        bool full() const { return size == MAX_SIZE; }

        Batch* insert(RowRef&& row_ref, Arena& pool) {
            if (full()) {
                auto batch = pool.alloc<Batch>();
                *batch = Batch(this);
                batch->insert(std::move(row_ref), pool);
                return batch;
            }

            row_refs[size++] = std::move(row_ref);
            return this;
        }
    };

    class ForwardIterator {
    public:
        ForwardIterator(const RowRefList* begin)
                : root(begin), first(true), batch(root->next), position(0) {}

        const RowRef* operator->() const {
            if (first) return root;
            return &batch->row_refs[position];
        }

        void operator++() {
            if (first) {
                first = false;
                return;
            }

            if (batch) {
                ++position;
                if (position >= batch->size) {
                    batch = batch->next;
                    position = 0;
                }
            }
        }

        bool ok() const { return first || (batch && position < batch->size); }

    private:
        const RowRefList* root;
        bool first;
        Batch* batch;
        size_t position;
    };

    RowRefList() {}
    RowRefList(const Block* block_, size_t row_num_) : RowRef(block_, row_num_) {}

    ForwardIterator begin() const { return ForwardIterator(this); }

    /// insert element after current one
    void insert(RowRef&& row_ref, Arena& pool) {
        if (!next) {
            next = pool.alloc<Batch>();
            *next = Batch(nullptr);
        }
        next = next->insert(std::move(row_ref), pool);
    }

private:
    Batch* next = nullptr;
};

// using MapI32 = doris::vectorized::HashMap<UInt32, MappedAll, HashCRC32<UInt32>>;
// using I32KeyType = doris::vectorized::ColumnsHashing::HashMethodOneNumber<MapI32::value_type, MappedAll, UInt32, false>;
} // namespace doris::vectorized
