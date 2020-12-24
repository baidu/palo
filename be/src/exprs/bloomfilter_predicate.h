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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_BLOOM_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_BLOOM_PREDICATE_H
#include <memory>
#include <string>

#include "common/object_pool.h"
#include "exprs/predicate.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"

namespace doris {
/// only used in Runtime Filter
class BloomFilterFuncBase {
public:
    BloomFilterFuncBase(MemTracker* tracker) : _tracker(tracker) {};
    virtual ~BloomFilterFuncBase() { _tracker->Release(_bloom_filter_alloced); }

    // init a bloom filter with expect element num
    virtual Status init(int64_t expect_num = 4096, double fpp = 0.05) {
        DCHECK(expect_num >= 0);
        // we need alloc 'optimal_bit_num(expect_num,fpp) / 8' bytes
        _bloom_filter_alloced =
                doris::segment_v2::BloomFilter::optimal_bit_num(expect_num, fpp) / 8;
        Status st = doris::segment_v2::BloomFilter::create(
                doris::segment_v2::BloomFilterAlgorithmPB::BLOCK_BLOOM_FILTER, &_bloom_filter);
        // status is always true if we use valid BloomFilterAlgorithmPB 
        DCHECK(st.ok());
        st = _bloom_filter->init(_bloom_filter_alloced,
                                 doris::segment_v2::HashStrategyPB::HASH_MURMUR3_X64_64);
        // status is always true if we use HASH_MURMUR3_X64_64
        DCHECK(st.ok());
        _tracker->Consume(_bloom_filter_alloced);
        return st;
    }
    virtual void insert(void* data) = 0;
    virtual bool find(void* data) = 0;
    /// create a bloom filter function
    static BloomFilterFuncBase* create_bloom_filter(MemTracker* tracker, PrimitiveType type);

protected:
    MemTracker* _tracker;
    // bloom filter size
    int32_t _bloom_filter_alloced;
    std::unique_ptr<doris::segment_v2::BloomFilter> _bloom_filter;
};

template <class T>
class BloomFilterFunc : public BloomFilterFuncBase {
public:
    BloomFilterFunc(MemTracker* tracker) : BloomFilterFuncBase(tracker) {}

    ~BloomFilterFunc() = default;

    virtual void insert(void* data) {
        DCHECK(_bloom_filter != nullptr);
        _bloom_filter->add_bytes((char*)data, sizeof(T));
    }

    virtual bool find(void* data) {
        DCHECK(_bloom_filter != nullptr);
        return _bloom_filter->test_bytes((char*)data, sizeof(T));
    }
};

template <>
class BloomFilterFunc<StringValue> : public BloomFilterFuncBase {
public:
    BloomFilterFunc(MemTracker* tracker) : BloomFilterFuncBase(tracker) {}

    ~BloomFilterFunc() = default;

    virtual void insert(void* data) {
        DCHECK(_bloom_filter != nullptr);
        StringValue* value = reinterpret_cast<StringValue*>(data);
        _bloom_filter->add_bytes(value->ptr, value->len);
    }

    virtual bool find(void* data) {
        DCHECK(_bloom_filter != nullptr);
        StringValue* value = reinterpret_cast<StringValue*>(data);
        return _bloom_filter->test_bytes(value->ptr, value->len);
    }
};

// BloomFilterPredicate only used in runtime filter
class BloomFilterPredicate : public Predicate {
public:
    virtual ~BloomFilterPredicate();
    BloomFilterPredicate(const TExprNode& node);
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new BloomFilterPredicate(*this));
    }
    Status prepare(RuntimeState* state, BloomFilterFuncBase* bloomfilterfunc);
    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;
    virtual Status open(RuntimeState* state, ExprContext* context,
                        FunctionContext::FunctionStateScope scope) override;

protected:
    friend class Expr;
    virtual std::string debug_string() const override;

private:
    bool _is_prepare;
    // if we set always = true, we will skip bloom filter
    bool _always_true;
    /// TODO: statistic filter rate in the profile
    int64_t _filtered_rows;
    int64_t _scan_rows;
    
    std::shared_ptr<BloomFilterFuncBase> _filter;
    bool _has_calculate_filter = false;
    // loop size must be power of 2
    constexpr static int64_t _loop_size = 8192;
    // if filter rate less than this, bloom filter will set always true
    constexpr static float _expect_filter_rate = 0.2f;
};
} // namespace doris
#endif
