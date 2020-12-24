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

#include "runtime_filter.h"

#include <memory>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/binary_predicate.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/expr_context.h"
#include "exprs/hybrid_set.h"
#include "exprs/in_predicate.h"
#include "exprs/literal.h"
#include "exprs/predicate.h"
#include "runtime/type_limit.h"

namespace doris {
// only used in Runtime Filter
class MinMaxFuncBase {
public:
    virtual void insert(void* data) = 0;
    virtual bool find(void* data) = 0;
    virtual bool is_empty() = 0;
    virtual const void* get_max() = 0;
    virtual const void* get_min() = 0;
    // create min-max filter function
    static MinMaxFuncBase* create_minmax_filter(PrimitiveType type);
};

template <class T>
class MinMaxNumFunc : public MinMaxFuncBase {
public:
    MinMaxNumFunc() = default;
    ~MinMaxNumFunc() = default;

    virtual void insert(void* data) {
        if (data == nullptr) return;
        T val_data = *reinterpret_cast<T*>(data);
        if (_empty) {
            _min = val_data;
            _max = val_data;
            _empty = false;
            return;
        }

        if (val_data < _min) {
            _min = val_data;
        } else if (val_data > _max) {
            _max = val_data;
        }
    }

    virtual bool find(void* data) {
        if (data == nullptr) {
            return false;
        }
        T val_data = *reinterpret_cast<T*>(data);
        return val_data >= _min && val_data <= _max;
    }

    virtual bool is_empty() { return _empty; }

    virtual const void* get_max() { return &_max; }

    virtual const void* get_min() { return &_min; }

private:
    T _max = type_limit<T>::min();
    T _min = type_limit<T>::max();
    // we use _empty to avoid compare twice
    bool _empty = true;
};

MinMaxFuncBase* MinMaxFuncBase::create_minmax_filter(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return new (std::nothrow) MinMaxNumFunc<bool>();

    case TYPE_TINYINT:
        return new (std::nothrow) MinMaxNumFunc<int8_t>();

    case TYPE_SMALLINT:
        return new (std::nothrow) MinMaxNumFunc<int16_t>();

    case TYPE_INT:
        return new (std::nothrow) MinMaxNumFunc<int32_t>();

    case TYPE_BIGINT:
        return new (std::nothrow) MinMaxNumFunc<int64_t>();

    case TYPE_FLOAT:
        return new (std::nothrow) MinMaxNumFunc<float>();

    case TYPE_DOUBLE:
        return new (std::nothrow) MinMaxNumFunc<double>();

    case TYPE_DATE:
    case TYPE_DATETIME:
        return new (std::nothrow) MinMaxNumFunc<DateTimeValue>();

    case TYPE_DECIMAL:
        return new (std::nothrow) MinMaxNumFunc<DecimalValue>();

    case TYPE_DECIMALV2:
        return new (std::nothrow) MinMaxNumFunc<DecimalV2Value>();

    case TYPE_LARGEINT:
        return new (std::nothrow) MinMaxNumFunc<__int128>();

    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return new (std::nothrow) MinMaxNumFunc<StringValue>();
    default:
        DCHECK(false) << "Invalid type.";
    }
    return NULL;
}

// PrimitiveType->TExprNodeType
static TExprNodeType::type get_expr_node_type(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return TExprNodeType::BOOL_LITERAL;

    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return TExprNodeType::INT_LITERAL;

    case TYPE_LARGEINT:
        return TExprNodeType::LARGE_INT_LITERAL;
        break;

    case TYPE_NULL:
        return TExprNodeType::NULL_LITERAL;

    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_TIME:
        return TExprNodeType::FLOAT_LITERAL;
        break;

    case TYPE_DECIMAL:
    case TYPE_DECIMALV2:
        return TExprNodeType::DECIMAL_LITERAL;

    case TYPE_DATETIME:
        return TExprNodeType::DATE_LITERAL;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
        return TExprNodeType::STRING_LITERAL;

    default:
        DCHECK(false) << "Invalid type.";
        return TExprNodeType::NULL_LITERAL;
    }
}

static TTypeDesc create_type_desc(PrimitiveType type) {
    TTypeDesc type_desc;
    std::vector<TTypeNode> node_type;
    node_type.emplace_back();
    TScalarType scalarType;
    scalarType.__set_type(to_thrift(type));
    scalarType.__set_len(-1);
    node_type.back().__set_scalar_type(scalarType);
    type_desc.__set_types(node_type);
    return type_desc;
}

// only used to push down to olap engine
Expr* create_literal(ObjectPool* pool, PrimitiveType type, const void* data) {
    TExprNode node;

    switch (type) {
    case TYPE_BOOLEAN: {
        TBoolLiteral boolLiteral;
        boolLiteral.__set_value(*reinterpret_cast<const bool*>(data));
        node.__set_bool_literal(boolLiteral);
        break;
    }
    case TYPE_TINYINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int8_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_SMALLINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int16_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_INT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int32_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_BIGINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int64_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_LARGEINT: {
        TLargeIntLiteral largeIntLiteral;
        largeIntLiteral.__set_value(
                LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(data)));
        node.__set_large_int_literal(largeIntLiteral);
        break;
    }
    case TYPE_FLOAT: {
        TFloatLiteral floatLiteral;
        floatLiteral.__set_value(*reinterpret_cast<const float*>(data));
        node.__set_float_literal(floatLiteral);
        break;
    }
    case TYPE_DOUBLE: {
        TFloatLiteral floatLiteral;
        floatLiteral.__set_value(*reinterpret_cast<const double*>(data));
        node.__set_float_literal(floatLiteral);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        TDateLiteral dateLiteral;
        char convert_buffer[30];
        reinterpret_cast<const DateTimeValue*>(data)->to_string(convert_buffer);
        dateLiteral.__set_value(convert_buffer);
        node.__set_date_literal(dateLiteral);
        break;
    }
    case TYPE_DECIMAL: {
        TDecimalLiteral decimalLiteral;
        decimalLiteral.__set_value(reinterpret_cast<const DecimalValue*>(data)->to_string());
        node.__set_decimal_literal(decimalLiteral);
        break;
    }
    case TYPE_DECIMALV2: {
        TDecimalLiteral decimalLiteral;
        decimalLiteral.__set_value(reinterpret_cast<const DecimalV2Value*>(data)->to_string());
        node.__set_decimal_literal(decimalLiteral);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(data);
        TStringLiteral tstringLiteral;
        tstringLiteral.__set_value(std::string(string_value->ptr, string_value->len));
        node.__set_string_literal(tstringLiteral);
        break;
    }
    default:
        DCHECK(false);
        return NULL;
    }
    node.__set_node_type(get_expr_node_type(type));
    node.__set_type(create_type_desc(type));
    return pool->add(new Literal(node));
}

BinaryPredicate* create_bin_predicate(PrimitiveType prim_type, TExprOpcode::type opcode) {
    TExprNode node;
    TScalarType tscalar_type;
    tscalar_type.__set_type(TPrimitiveType::BOOLEAN);
    TTypeNode ttype_node;
    ttype_node.__set_type(TTypeNodeType::SCALAR);
    ttype_node.__set_scalar_type(tscalar_type);
    TTypeDesc t_type_desc;
    t_type_desc.types.push_back(ttype_node);
    node.__set_type(t_type_desc);
    node.__set_opcode(opcode);
    node.__set_child_type(to_thrift(prim_type));
    node.__set_num_children(2);
    node.__set_output_scale(-1);
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    return (BinaryPredicate*)BinaryPredicate::from_thrift(node);
}

// This class will help us build really pushdown expressions
class RuntimePredicateWrapper {
public:
    RuntimePredicateWrapper(RuntimeState* state, MemTracker* tracker, ObjectPool* pool)
            : _state(state),
              _tracker(tracker),
              _pool(pool),
              _filter_type(RuntimeFilterType::UNKNOWN_FILTER),
              _expr_ctx(nullptr) {}

    Status init(RuntimeFilterType filter_type, ExprContext* expr_ctx, int64_t hash_table_size) {
        _filter_type = filter_type;
        _expr_ctx = expr_ctx;

        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _hybrid_set.reset(HybridSetBase::create_set(_expr_ctx->root()->type().type));
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _minmax_func.reset(
                    MinMaxFuncBase::create_minmax_filter(_expr_ctx->root()->type().type));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _bloomfilter_func.reset(BloomFilterFuncBase::create_bloom_filter(
                    _tracker, _expr_ctx->root()->type().type));
            RETURN_IF_ERROR(_bloomfilter_func->init(hash_table_size));
            break;
        }
        default:
            DCHECK(false);
            return Status::InvalidArgument("unknow filter type");
        }
        return Status::OK();
    }

    void insert(void* data) {
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _hybrid_set->insert(data);
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _minmax_func->insert(data);
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            DCHECK(_bloomfilter_func != nullptr);
            _bloomfilter_func->insert(data);
            break;
        }
        default:
            DCHECK(false);
            break;
        }
    }
    
    template <class T>
    Status get_push_context(T* container) {
        DCHECK(container != nullptr);
        PrimitiveType expr_primitive_type = _expr_ctx->root()->type().type;

        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            TTypeDesc type_desc = create_type_desc(_expr_ctx->root()->type().type);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::IN_PRED);
            node.in_predicate.__set_is_not_in(false);
            node.__set_opcode(TExprOpcode::FILTER_IN);
            node.__isset.vector_opcode = true;
            node.__set_vector_opcode(to_in_opcode(expr_primitive_type));
            auto in_pred = _pool->add(new InPredicate(node));
            RETURN_IF_ERROR(in_pred->prepare(_state, _hybrid_set.release()));
            in_pred->add_child(Expr::copy(_pool, _expr_ctx->root()));
            ExprContext* ctx = _pool->add(new ExprContext(in_pred));
            container->push_back(ctx);
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            // create max filter
            auto max_pred = create_bin_predicate(expr_primitive_type, TExprOpcode::LE);
            auto max_literal = create_literal(_pool, expr_primitive_type, _minmax_func->get_max());
            max_pred->add_child(Expr::copy(_pool, _expr_ctx->root()));
            max_pred->add_child(max_literal);
            container->push_back(_pool->add(new ExprContext(max_pred)));
            // create min filter
            auto min_pred = create_bin_predicate(expr_primitive_type, TExprOpcode::GE);
            auto min_literal = create_literal(_pool, expr_primitive_type, _minmax_func->get_min());
            min_pred->add_child(Expr::copy(_pool, _expr_ctx->root()));
            min_pred->add_child(min_literal);
            container->push_back(_pool->add(new ExprContext(min_pred)));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            // create a bloom filter
            TTypeDesc type_desc = create_type_desc(_expr_ctx->root()->type().type);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::BLOOM_PRED);
            node.__set_opcode(TExprOpcode::RT_FILTER);
            node.__isset.vector_opcode = true;
            node.__set_vector_opcode(to_in_opcode(expr_primitive_type));
            auto bloom_pred = _pool->add(new BloomFilterPredicate(node));
            RETURN_IF_ERROR(bloom_pred->prepare(_state, _bloomfilter_func.release()));
            bloom_pred->add_child(Expr::copy(_pool, _expr_ctx->root()));
            ExprContext* ctx = _pool->add(new ExprContext(bloom_pred));
            container->push_back(ctx);
            break;
        }
        default:
            DCHECK(false);
            break;
        }
        return Status::OK();
    }

private:
    RuntimeState* _state;
    MemTracker* _tracker;
    ObjectPool* _pool;
    RuntimeFilterType _filter_type;
    std::unique_ptr<MinMaxFuncBase> _minmax_func;
    std::unique_ptr<HybridSetBase> _hybrid_set;
    std::unique_ptr<BloomFilterFuncBase> _bloomfilter_func;
    ExprContext* _expr_ctx;
};

RuntimeFilter::RuntimeFilter(RuntimeState* state, MemTracker* mem_tracker, ObjectPool* pool)
        : _state(state), _mem_tracker(mem_tracker), _pool(pool) {}

RuntimeFilter::~RuntimeFilter() {}

Status RuntimeFilter::create_runtime_predicate(RuntimeFilterType filter_type, size_t prob_index,
                                               ExprContext* prob_expr_ctx,
                                               int64_t hash_table_size) {
    switch (filter_type) {
    case RuntimeFilterType::IN_FILTER:
    case RuntimeFilterType::MINMAX_FILTER:
    case RuntimeFilterType::BLOOM_FILTER: {
        RuntimePredicateWrapper* wrapper =
                _pool->add(new RuntimePredicateWrapper(_state, _mem_tracker, _pool));
        // if wrapper init error
        // we will ignore this filter
        RETURN_IF_ERROR(wrapper->init(filter_type, prob_expr_ctx, hash_table_size));
        _runtime_preds[prob_index].push_back(wrapper);
        break;
    }
    default:
        DCHECK(false) << "Invalid type.";
        return Status::NotSupported("not support filter type");
    }
    return Status::OK();
}

void RuntimeFilter::insert(int prob_index, void* data) {
    auto iter = _runtime_preds.find(prob_index);
    if (iter != _runtime_preds.end()) {
        for (auto filter : iter->second) {
            filter->insert(data);
        }
    }
}

Status RuntimeFilter::get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs) {
    DCHECK(push_expr_ctxs != nullptr);
    for (auto& iter : _runtime_preds) {
        for (auto pred : iter.second) {
            pred->get_push_context(push_expr_ctxs);
        }
    }
    return Status::OK();
}

} // namespace doris
