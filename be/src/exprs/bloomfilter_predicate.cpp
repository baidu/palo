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

#include "exprs/bloomfilter_predicate.h"

#include <sstream>

#include "exprs/anyval_util.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.hpp"

namespace doris {
BloomFilterFuncBase* BloomFilterFuncBase::create_bloom_filter(MemTracker* tracker,
                                                              PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return new (std::nothrow) BloomFilterFunc<bool>(tracker);

    case TYPE_TINYINT:
        return new (std::nothrow) BloomFilterFunc<int8_t>(tracker);

    case TYPE_SMALLINT:
        return new (std::nothrow) BloomFilterFunc<int16_t>(tracker);

    case TYPE_INT:
        return new (std::nothrow) BloomFilterFunc<int32_t>(tracker);

    case TYPE_BIGINT:
        return new (std::nothrow) BloomFilterFunc<int64_t>(tracker);

    case TYPE_FLOAT:
        return new (std::nothrow) BloomFilterFunc<float>(tracker);

    case TYPE_DOUBLE:
        return new (std::nothrow) BloomFilterFunc<double>(tracker);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return new (std::nothrow) BloomFilterFunc<DateTimeValue>(tracker);

    case TYPE_DECIMAL:
        return new (std::nothrow) BloomFilterFunc<DecimalValue>(tracker);

    case TYPE_DECIMALV2:
        return new (std::nothrow) BloomFilterFunc<DecimalV2Value>(tracker);

    case TYPE_LARGEINT:
        return new (std::nothrow) BloomFilterFunc<__int128>(tracker);

    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return new (std::nothrow) BloomFilterFunc<StringValue>(tracker);

    default:
        return nullptr;
    }

    return nullptr;
}

BloomFilterPredicate::BloomFilterPredicate(const TExprNode& node)
        : Predicate(node),
          _is_prepare(false),
          _always_true(false),
          _filtered_rows(0),
          _scan_rows(0) {}

BloomFilterPredicate::~BloomFilterPredicate() {}

Status BloomFilterPredicate::prepare(RuntimeState* state, BloomFilterFuncBase* filter) {
    if (_is_prepare) {
        return Status::OK();
    }
    _filter.reset(filter);
    if (NULL == _filter.get()) {
        return Status::InternalError("Unknown column type.");
    }
    _is_prepare = true;
    return Status::OK();
}

std::string BloomFilterPredicate::debug_string() const {
    std::stringstream out;
    out << "BloomFilterPredicate()";
    return out.str();
}

BooleanVal BloomFilterPredicate::get_boolean_val(ExprContext* ctx, TupleRow* row) {
    if (_always_true) {
        return BooleanVal(true);
    }
    void* lhs_slot = ctx->get_value(_children[0], row);
    if (lhs_slot == NULL) {
        return BooleanVal::null();
    }
    _scan_rows++;
    if (_filter->find(lhs_slot)) {
        return BooleanVal(true);
    }
    _filtered_rows++;

    if (!_has_calculate_filter && _scan_rows % _loop_size == 0) {
        float rate = (float)_filtered_rows / _scan_rows;
        if (rate < _expect_filter_rate) {
            _always_true = true;
        }
        _has_calculate_filter = true;
    }
    return BooleanVal(false);
}

Status BloomFilterPredicate::open(RuntimeState* state, ExprContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    Expr::open(state, context, scope);
    return Status::OK();
}

} // namespace doris
