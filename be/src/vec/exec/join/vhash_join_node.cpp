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

#include "vec/exec/join/vhash_join_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "util/defer_op.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

using ProfileCounter = RuntimeProfile::Counter;
template <class HashTableContext, bool has_null_map>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs,
                          HashJoinNode* join_node)
            : _rows(rows),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _join_node(join_node) {}

    Status operator()(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        Defer defer {[&]() {
            int64_t bucket_size = hash_table_ctx.hash_table.get_buffer_size_in_cells();
            COUNTER_SET(_join_node->_build_buckets_counter, bucket_size);
        }};

        KeyGetter key_getter(_build_raw_ptrs, _join_node->_build_key_sz, nullptr);

        SCOPED_TIMER(_join_node->_build_table_insert_timer);
        for (size_t k = 0; k < _rows; ++k) {
            // TODO: make this as constexpr
            if constexpr (has_null_map) {
                if ((*null_map)[k]) {
                    continue;
                }
            }

            auto emplace_result =
                    key_getter.emplace_key(hash_table_ctx.hash_table, k, _join_node->_arena);
            if (k + 1 < _rows) {
                key_getter.prefetch(hash_table_ctx.hash_table, k + 1, _join_node->_arena);
            }

            if (emplace_result.is_inserted()) {
                new (&emplace_result.get_mapped()) Mapped({&_acquired_block, k});
            } else {
                /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                emplace_result.get_mapped().insert({&_acquired_block, k}, _join_node->_arena);
            }
        }

        return Status::OK();
    }

private:
    const int _rows;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    HashJoinNode* _join_node;
};

template <class HashTableContext, bool has_null_map>
struct ProcessHashTableProbe {
    ProcessHashTableProbe(HashJoinNode* join_node, int batch_size, int probe_rows)
            : _join_node(join_node),
              _left_table_data_types(join_node->_left_table_data_types),
              _right_table_data_types(join_node->_right_table_data_types),
              _batch_size(batch_size),
              _probe_rows(probe_rows),
              _probe_block(join_node->_probe_block),
              _probe_index(join_node->_probe_index),
              _num_rows_returned(join_node->_num_rows_returned),
              _probe_raw_ptrs(join_node->_probe_columns),
              _arena(join_node->_arena),
              _rows_returned_counter(join_node->_rows_returned_counter) {}

    Status operator()(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map,
                      MutableBlock& mutable_block, Block* output_block) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        KeyGetter key_getter(_probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);

        IColumn::Offsets offset_data;
        auto& mcol = mutable_block.mutable_columns();
        offset_data.assign(_probe_rows, (uint32_t)0);

        int right_col_idx = _left_table_data_types.size();
        int current_offset = 0;

        for (; _probe_index < _probe_rows;) {
            // ignore null rows
            if constexpr (has_null_map) {
                if ((*null_map)[_probe_index]) {
                    offset_data[_probe_index++] = current_offset;
                    continue;
                }
            }
            auto find_result = key_getter.find_key(hash_table_ctx.hash_table, _probe_index, _arena);

            if (_probe_index + 1 < _probe_rows) {
                key_getter.prefetch(hash_table_ctx.hash_table, _probe_index + 1, _arena);
            }

            if (find_result.is_found()) {
                auto& mapped = find_result.get_mapped();
                for (auto it = mapped.begin(); it.ok(); ++it) {
                    int size = _right_table_data_types.size();
                    for (size_t j = 0; j < size; ++j) {
                        auto& column = *it->block->get_by_position(j).column;
                        mcol[j + right_col_idx]->insert_from(column, it->row_num);
                    }
                    ++current_offset;
                }
            }

            offset_data[_probe_index++] = current_offset;

            if (current_offset >= _batch_size) {
                break;
            }
        }

        for (int i = _probe_index; i < _probe_rows; ++i) {
            offset_data[i] = current_offset;
        }

        output_block->swap(mutable_block.to_block());
        for (int i = 0; i < right_col_idx; ++i) {
            auto& column = _probe_block.get_by_position(i).column;
            output_block->get_by_position(i).column = column->replicate(offset_data);
        }

        if (_join_node->_vconjunct_ctx_ptr) {
            int result_column_id = -1;
            int orig_columns = output_block->columns();
            (*_join_node->_vconjunct_ctx_ptr)->execute(output_block, &result_column_id);
            Block::filter_block(output_block, result_column_id, orig_columns);
        }

        int64_t m = output_block->rows();
        COUNTER_UPDATE(_rows_returned_counter, m);
        _num_rows_returned += m;

        return Status::OK();
    }

private:
    HashJoinNode* _join_node;
    const DataTypes& _left_table_data_types;
    const DataTypes& _right_table_data_types;
    const int _batch_size;
    const size_t _probe_rows;
    const Block& _probe_block;
    int& _probe_index;
    int64_t& _num_rows_returned;
    ColumnRawPtrs& _probe_raw_ptrs;
    Arena& _arena;

    ProfileCounter* _rows_returned_counter;
};

// now we only support inner join
HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _join_op(tnode.hash_join_node.join_op),
          _hash_table_rows(0) {}

HashJoinNode::~HashJoinNode() {}

Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    if (tnode.hash_join_node.join_op != TJoinOp::INNER_JOIN) {
        return Status::InternalError("Do not support unless inner join");
    }
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;

    for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjuncts[i].left, &ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjuncts[i].right, &ctx));
        _build_expr_ctxs.push_back(ctx);
        if (eq_join_conjuncts[i].__isset.opcode &&
            eq_join_conjuncts[i].opcode == TExprOpcode::EQ_FOR_NULL) {
            _is_null_safe_eq_join.push_back(true);
        } else {
            _is_null_safe_eq_join.push_back(false);
        }
    }

    RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, tnode.hash_join_node.other_join_conjuncts,
                                             &_other_join_conjunct_ctxs));

    if (!_other_join_conjunct_ctxs.empty()) {
        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        _build_unique = false;
    }

    return Status::OK();
}

Status HashJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    // Build phase
    auto build_phase_profile = runtime_profile()->create_child("BuildPhase", true, true);
    runtime_profile()->add_child(build_phase_profile, false, nullptr);
    _build_timer = ADD_TIMER(build_phase_profile, "BuildTime");
    _build_table_timer = ADD_TIMER(build_phase_profile, "BuildTableTime");
    _build_hash_calc_timer = ADD_TIMER(build_phase_profile, "BuildHashCalcTime");
    _build_bucket_calc_timer = ADD_TIMER(build_phase_profile, "BuildBucketCalcTime");
    _build_expr_call_timer = ADD_TIMER(build_phase_profile, "BuildExprCallTime");
    _build_table_insert_timer = ADD_TIMER(build_phase_profile, "BuildTableInsertTime");
    _build_table_spread_timer = ADD_TIMER(build_phase_profile, "BuildTableSpreadTime");
    _build_table_expanse_timer = ADD_TIMER(build_phase_profile, "BuildTableExpanseTime");
    _build_acquire_block_timer = ADD_TIMER(build_phase_profile, "BuildAcquireBlockTime");
    _build_rows_counter = ADD_COUNTER(build_phase_profile, "BuildRows", TUnit::UNIT);

    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");
    // Probe phase
    auto probe_phase_profile = runtime_profile()->create_child("ProbePhase", true, true);
    _probe_timer = ADD_TIMER(probe_phase_profile, "ProbeTime");
    _probe_gather_timer = ADD_TIMER(probe_phase_profile, "ProbeGatherTime");
    _probe_next_timer = ADD_TIMER(probe_phase_profile, "ProbeFindNextTime");
    _probe_diff_timer = ADD_TIMER(probe_phase_profile, "ProbeDiffTime");
    _probe_expr_call_timer = ADD_TIMER(probe_phase_profile, "ProbeExprCallTime");
    _probe_hash_calc_timer = ADD_TIMER(probe_phase_profile, "ProbeHashCalcTime");
    _probe_select_miss_timer = ADD_TIMER(probe_phase_profile, "ProbeSelectMissTime");
    _probe_select_zero_timer = ADD_TIMER(probe_phase_profile, "ProbeSelectZeroTime");
    _probe_rows_counter = ADD_COUNTER(probe_phase_profile, "ProbeRows", TUnit::UNIT);
    _build_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);

    RETURN_IF_ERROR(
            VExpr::prepare(_build_expr_ctxs, state, child(1)->row_desc(), expr_mem_tracker()));
    RETURN_IF_ERROR(
            VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    RETURN_IF_ERROR(
            VExpr::prepare(_other_join_conjunct_ctxs, state, _row_descriptor, expr_mem_tracker()));

    // right table data types
    _right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());

    // Hash Table Init
    _hash_table_init();
    return Status::OK();
}

Status HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

Status HashJoinNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented HashJoin Node::get_next scalar");
}

Status HashJoinNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_probe_timer);
    size_t probe_rows = _probe_block.rows();
    if (probe_rows == 0 || _probe_index == probe_rows) {
        _probe_index = 0;
        _probe_block.clear();

        do {
            RETURN_IF_ERROR(child(0)->get_next(state, &_probe_block, eos));
        } while (_probe_block.rows() == 0 && !(*eos));

        probe_rows = _probe_block.rows();
        if (probe_rows == 0) {
            *eos = true;
            return Status::OK();
        }

        int probe_expr_ctxs_sz = _probe_expr_ctxs.size();
        _probe_columns.resize(probe_expr_ctxs_sz);

        for (int i = 0; i < probe_expr_ctxs_sz; ++i) {
            int result_id = -1;
            _probe_expr_ctxs[i]->execute(&_probe_block, &result_id);
            DCHECK_GE(result_id, 0);
            _probe_columns[i] = _probe_block.get_by_position(result_id).column.get();
        }

        if (_null_map_column == nullptr) {
            _null_map_column = ColumnUInt8::create();
        }
        _null_map_column->get_data().assign(probe_rows, (uint8_t)0);

        Status st = std::visit(
                [&](auto&& arg) -> Status {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        auto& null_map_val = _null_map_column->get_data();
                        return extract_eq_join_column<
                                HashTableCtxType::could_handle_asymmetric_null>(
                                _probe_expr_ctxs, _probe_block, null_map_val, _probe_columns,
                                _probe_has_null);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                    __builtin_unreachable();
                },
                _hash_table_variants);

        RETURN_IF_ERROR(st);
    }

    MutableBlock mutable_block(VectorizedUtils::create_empty_columnswithtypename(row_desc()));
    output_block->clear();

    Status st;

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    if (_probe_has_null) {
                        ProcessHashTableProbe<HashTableCtxType, true> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);

                        st = process_hashtable_ctx(arg, &_null_map_column->get_data(),
                                                   mutable_block, output_block);
                    } else {
                        ProcessHashTableProbe<HashTableCtxType, false> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);

                        st = process_hashtable_ctx(arg, &_null_map_column->get_data(),
                                                   mutable_block, output_block);
                    }
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return st;
}

Status HashJoinNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(VExpr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_other_join_conjunct_ctxs, state));

    RETURN_IF_ERROR(_hash_table_build(state));
    RETURN_IF_ERROR(child(0)->open(state));

    return Status::OK();
}

Status HashJoinNode::_hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(1)->open(state));
    SCOPED_TIMER(_build_timer);
    Block block;

    bool eos = false;
    while (!eos) {
        block.clear();
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(child(1)->get_next(state, &block, &eos));
        RETURN_IF_ERROR(_process_build_block(block));
    }
    return Status::OK();
}

template <bool asymmetric_null>
Status HashJoinNode::extract_eq_join_column(VExprContexts& exprs, Block& block, NullMap& null_map,
                                            ColumnRawPtrs& raw_ptrs, bool& has_null) {
    for (size_t i = 0; i < exprs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        RETURN_IF_ERROR(exprs[i]->execute(&block, &result_col_id));

        if (_is_null_safe_eq_join[i]) {
            raw_ptrs[i] = block.get_by_position(result_col_id).column.get();
        } else {
            auto column = block.get_by_position(result_col_id).column.get();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
                auto& col_nested = nullable->get_nested_column();
                auto& col_nullmap = nullable->get_null_map_data();
                VectorizedUtils::update_null_map(null_map, col_nullmap);
                has_null = true;

                if constexpr (asymmetric_null) {
                    raw_ptrs[i] = nullable;
                } else {
                    raw_ptrs[i] = &col_nested;
                }
            } else {
                raw_ptrs[i] = column;
            }
        }
    }
    return Status::OK();
}

Status HashJoinNode::_process_build_block(Block& block) {
    SCOPED_TIMER(_build_table_timer);
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }
    auto& acquired_block = _acquire_list.acquire(std::move(block));

    materialize_block_inplace(acquired_block);

    ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

    NullMap null_map_val(rows);
    null_map_val.assign(rows, (uint8_t)0);
    bool has_null = false;

    // Get the key column that needs to be built
    Status st = std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    return extract_eq_join_column<HashTableCtxType::could_handle_asymmetric_null>(
                            _build_expr_ctxs, acquired_block, null_map_val, raw_ptrs, has_null);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
                __builtin_unreachable();
            },
            _hash_table_variants);

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    if (has_null) {
                        ProcessHashTableBuild<HashTableCtxType, true> hash_table_build_process(
                                rows, acquired_block, raw_ptrs, this);
                        st = hash_table_build_process(arg, &null_map_val);
                    } else {
                        ProcessHashTableBuild<HashTableCtxType, false> hash_table_build_process(
                                rows, acquired_block, raw_ptrs, this);
                        st = hash_table_build_process(arg, &null_map_val);
                    }
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return st;
}

void HashJoinNode::_hash_table_init() {
    if (_build_expr_ctxs.size() == 1 && !_is_null_safe_eq_join[0]) {
        // Single column optimization
        switch (_build_expr_ctxs[0]->root()->result_type()) {
        case TYPE_TINYINT:
            _hash_table_variants.emplace<I8HashTableContext>();
            break;
        case TYPE_SMALLINT:
            _hash_table_variants.emplace<I16HashTableContext>();
            break;
        case TYPE_INT:
            _hash_table_variants.emplace<I32HashTableContext>();
            break;
        case TYPE_BIGINT:
            _hash_table_variants.emplace<I64HashTableContext>();
            break;
        default:
            _hash_table_variants.emplace<SerializedHashTableContext>();
        }
        return;
    }

    bool use_fixed_key = true;
    bool has_null = false;
    int key_byte_size = 0;

    _probe_key_sz.resize(_probe_expr_ctxs.size());
    _build_key_sz.resize(_build_expr_ctxs.size());

    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        const auto vexpr = _build_expr_ctxs[i]->root();
        const auto& data_type = vexpr->data_type();
        auto result_type = vexpr->result_type();

        has_null |= data_type->is_nullable();
        _build_key_sz[i] = get_real_byte_size(result_type);
        _probe_key_sz[i] = _build_key_sz[i];

        key_byte_size += _build_key_sz[i];

        if (has_variable_type(result_type)) {
            use_fixed_key = false;
            break;
        }
    }

    if (key_byte_size > sizeof(UInt128)) {
        use_fixed_key = false;
    }

    if (use_fixed_key) {
        if (key_byte_size <= sizeof(UInt64)) {
            if (has_null) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<true>>();
            } else {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<false>>();
            }
        } else {
            DCHECK_LE(key_byte_size, sizeof(UInt128));
            if (has_null) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<true>>();
            } else {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<false>>();
            }
        }

    } else {
        _hash_table_variants.emplace<SerializedHashTableContext>();
    }
}

} // namespace doris::vectorized
