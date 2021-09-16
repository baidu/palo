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
#include <variant>

#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {

struct SerializedHashTableContext {
    using Mapped = RowRefList;
    using HashTable = HashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;

    static constexpr auto could_handle_asymmetric_null = false;
    HashTable hash_table;
};

// T should be UInt32 UInt64 UInt128
template <class T>
struct PrimaryTypeHashTableContext {
    using Mapped = RowRefList;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State =
            ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped, T, false>;
    static constexpr auto could_handle_asymmetric_null = false;

    HashTable hash_table;
};

// TODO: use FixedHashTable instead of HashTable
using I8HashTableContext = PrimaryTypeHashTableContext<UInt8>;
using I16HashTableContext = PrimaryTypeHashTableContext<UInt16>;

using I32HashTableContext = PrimaryTypeHashTableContext<UInt32>;
using I64HashTableContext = PrimaryTypeHashTableContext<UInt64>;

template <class T>
struct HashTableFunc;

template <>
struct HashTableFunc<UInt64> {
    using Func = HashCRC32<UInt64>;
};

template <>
struct HashTableFunc<UInt128> {
    using Func = UInt128HashCRC32;
};

template <class T, bool has_null>
struct FixedKeyHashTableContext {
    using Mapped = RowRefList;
    using HashTable = HashMap<T, Mapped, typename HashTableFunc<T>::Func>;
    using State = ColumnsHashing::HashMethodKeysFixed<typename HashTable::value_type, T, Mapped,
                                                      has_null, false>;
    static constexpr auto could_handle_asymmetric_null = true;
    HashTable hash_table;
};

template <bool has_null>
using I64FixedKeyHashTableContext = FixedKeyHashTableContext<UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContext = FixedKeyHashTableContext<UInt128, has_null>;

using HashTableVariants =
        std::variant<std::monostate, SerializedHashTableContext, I8HashTableContext,
                     I16HashTableContext, I32HashTableContext, I64HashTableContext,
                     I64FixedKeyHashTableContext<true>, I64FixedKeyHashTableContext<false>,
                     I128FixedKeyHashTableContext<true>, I128FixedKeyHashTableContext<false>>;

class VExprContext;

class HashJoinNode : public ::doris::ExecNode {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    using VExprContexts = std::vector<VExprContext*>;

    TJoinOp::type _join_op;
    // probe expr
    VExprContexts _probe_expr_ctxs;
    // build expr
    VExprContexts _build_expr_ctxs;
    // other expr
    VExprContexts _other_join_conjunct_ctxs;

    std::vector<bool> _is_null_safe_eq_join;

    DataTypes _right_table_data_types;
    DataTypes _left_table_data_types;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _build_table_timer;
    RuntimeProfile::Counter* _build_hash_calc_timer;
    RuntimeProfile::Counter* _build_bucket_calc_timer;
    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_spread_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _build_acquire_block_timer;
    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
    RuntimeProfile::Counter* _probe_hash_calc_timer;
    RuntimeProfile::Counter* _probe_gather_timer;
    RuntimeProfile::Counter* _probe_next_timer;
    RuntimeProfile::Counter* _probe_select_miss_timer;
    RuntimeProfile::Counter* _probe_select_zero_timer;
    RuntimeProfile::Counter* _probe_diff_timer;
    RuntimeProfile::Counter* _build_buckets_counter;

    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _build_rows_counter;
    RuntimeProfile::Counter* _probe_rows_counter;

    bool _build_unique;

    int64_t _hash_table_rows;

    Arena _arena;
    HashTableVariants _hash_table_variants;
    AcquireList<Block> _acquire_list;

    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    ColumnUInt8::MutablePtr _null_map_column;
    bool _probe_has_null = false;
    int _probe_index = -1;

    Sizes _probe_key_sz;
    Sizes _build_key_sz;

private:
    Status _hash_table_build(RuntimeState* state);
    Status _process_build_block(Block& block);

    template <bool asymmetric_null>
    Status extract_eq_join_column(VExprContexts& exprs, Block& block, NullMap& null_map,
                                  ColumnRawPtrs& raw_ptrs, bool& hash_null);

    void _hash_table_init();

    template <class HashTableContext, bool has_null_map>
    friend class ProcessHashTableBuild;

    template <class HashTableContext, bool has_null_map>
    friend class ProcessHashTableProbe;
};
} // namespace vectorized
} // namespace doris