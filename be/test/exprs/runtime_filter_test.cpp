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

#include "exprs/runtime_filter.h"

#include <array>
#include <memory>

#include "exprs/slot_ref.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris {
class RuntimeFilterTest : public testing::Test {
public:
    RuntimeFilterTest() {}
    virtual void SetUp() {
        ExecEnv* exec_env = ExecEnv::GetInstance();
        exec_env = nullptr;
        _runtime_stat.reset(
                new RuntimeState(_fragment_id, _query_options, _query_globals, exec_env));
        _runtime_stat->init_instance_mem_tracker();
        _runtime_filter.reset(new RuntimeFilter(
                _runtime_stat.get(), _runtime_stat->instance_mem_tracker().get(), &_obj_pool));
    }
    virtual void TearDown() { _obj_pool.clear(); }

private:
    ObjectPool _obj_pool;
    TUniqueId _fragment_id;
    TQueryOptions _query_options;
    TQueryGlobals _query_globals;

    std::unique_ptr<RuntimeState> _runtime_stat;
    std::unique_ptr<RuntimeFilter> _runtime_filter;
};

TEST_F(RuntimeFilterTest, create_runtime_filter_test) {
    RuntimeFilterType filter_type = RuntimeFilterType::BLOOM_FILTER;
    size_t prob_index = 0;
    SlotRef* expr = _obj_pool.add(new SlotRef(TYPE_INT, 0));
    ExprContext* prob_expr_ctx = _obj_pool.add(new ExprContext(expr));
    ExprContext* build_expr_ctx = _obj_pool.add(new ExprContext(expr));
    int64_t hash_table_size = 1024;
    Status status = _runtime_filter->create_runtime_predicate(filter_type, prob_index,
                                                              prob_expr_ctx, hash_table_size);
    ASSERT_TRUE(status.ok());
    std::array<TupleRow, 1024> tuple_rows;
    int generator_index = 0;
    std::generate(tuple_rows.begin(), tuple_rows.end(), [&]() {
        std::array<int, 2>* data = _obj_pool.add(new std::array<int, 2>());
        data->at(0) = data->at(1) = generator_index++;
        TupleRow row;
        row._tuples[0] = (Tuple*)data->data();
        return row;
    });
    for (TupleRow& row : tuple_rows) {
        void* val = build_expr_ctx->get_value(&row);
        _runtime_filter->insert(prob_index, val);
    }
    std::list<ExprContext*> expr_context_list;
    ASSERT_TRUE(_runtime_filter->get_push_expr_ctxs(&expr_context_list).ok());
    ASSERT_TRUE(!expr_context_list.empty());
    for (TupleRow& row : tuple_rows) {
        for (ExprContext* ctx : expr_context_list) {
            ASSERT_TRUE(ctx->get_boolean_val(&row).val);
        }
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}