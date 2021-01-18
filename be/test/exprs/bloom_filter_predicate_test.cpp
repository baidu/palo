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

#include <string>

#include "exprs/bloomfilter_predicate.h"
#include "gtest/gtest.h"
#include "runtime/string_value.h"

namespace doris {
class BloomFilterPredicateTest : public testing::Test {
public:
    BloomFilterPredicateTest() = default;
    virtual void SetUp() {}
    virtual void TearDown() {}
};

TEST_F(BloomFilterPredicateTest, bloom_filter_func_int_test) {
    auto tracker = MemTracker::CreateTracker();
    std::unique_ptr<BloomFilterFuncBase> func(
            BloomFilterFuncBase::create_bloom_filter(tracker.get(), PrimitiveType::TYPE_INT));
    ASSERT_TRUE(func->init().ok());
    const int data_size = 1024;
    int data[data_size];
    for (int i = 0; i < data_size; i++) {
        data[i] = i;
        func->insert((const void*)&data[i]);
    }
    for (int i = 0; i < data_size; i++) {
        ASSERT_TRUE(func->find((const void*)&data[i]));
    }
    // test not exist val
    int not_exist_val = 0x3355ff;
    ASSERT_FALSE(func->find((const void*)&not_exist_val));
}

TEST_F(BloomFilterPredicateTest, bloom_filter_func_stringval_test) {
    auto tracker = MemTracker::CreateTracker();
    std::unique_ptr<BloomFilterFuncBase> func(
            BloomFilterFuncBase::create_bloom_filter(tracker.get(), PrimitiveType::TYPE_VARCHAR));
    ASSERT_TRUE(func->init().ok());
    ObjectPool obj_pool;
    const int data_size = 1024;
    StringValue data[data_size];
    for (int i = 0; i < data_size; i++) {
        auto str = obj_pool.add(new std::string(std::to_string(i)));
        data[i] = StringValue(*str);
        func->insert((const void*)&data[i]);
    }
    for (int i = 0; i < data_size; i++) {
        ASSERT_TRUE(func->find((const void*)&data[i]));
    }
    // test not exist value
    std::string not_exist_str = "0x3355ff";
    StringValue not_exist_val(not_exist_str);
    ASSERT_FALSE(func->find((const void*)&not_exist_val));
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}