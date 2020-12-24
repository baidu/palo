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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_RUNTIME_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_RUNTIME_PREDICATE_H

#include <list>
#include <map>

#include "gen_cpp/Exprs_types.h"
#include "runtime/types.h"

namespace doris {
class Predicate;
class ObjectPool;
class ExprContext;
class RuntimeState;
class RuntimePredicateWrapper;
class MemTracker;

enum class RuntimeFilterType {
    UNKNOWN_FILTER = -1,
    IN_FILTER = 0,
    MINMAX_FILTER = 1,
    BLOOM_FILTER = 2
};

/// The runtimefilter is built in the join node.
/// The main purpose is to reduce the scanning amount of the
/// left table data according to the scanning results of the right table during the join process.
/// The runtimefilter will build some filter conditions.
/// that can be pushed down to node based on the results of the right table.
class RuntimeFilter {
public:
    RuntimeFilter(RuntimeState* state, MemTracker* mem_tracker, ObjectPool* pool);
    ~RuntimeFilter();
    // prob_index corresponds to the index of _probe_expr_ctxs in the join node
    // hash_table_size is the size of the hash_table
    Status create_runtime_predicate(RuntimeFilterType filter_type, size_t prob_index,
                                    ExprContext* prob_expr_ctx, int64_t hash_table_size);

    // We need to know the data corresponding to a prob_index when building an expression
    void insert(int prob_index, void* data);

    Status get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs);

private:
    // A mapping from prob_index to [runtime_predicate_warppers]
    std::map<int, std::list<RuntimePredicateWrapper*>> _runtime_preds;
    RuntimeState* _state;
    MemTracker* _mem_tracker;
    ObjectPool* _pool;
};
} // namespace doris

#endif
