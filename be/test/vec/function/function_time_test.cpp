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

#include <gtest/gtest.h>
#include <any>
#include <time.h>
#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "function_test_util.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {

using vectorized::Null;

TEST(TimestampFunctionsTest, day_of_week_test) {
    std::string func_name = "dayofweek";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::DateTime};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("2001-02-03 12:34:56")}, 7},
            {{std::string("2020-00-01 00:00:00")}, Null()},
            {{std::string("2020-01-00 00:00:00")}, Null()}
    };

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, day_of_month_test) {
    std::string func_name = "dayofmonth";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::DateTime};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("2020-00-01 00:00:00")}, Null()},
            {{std::string("2020-01-01 00:00:00")}, 1},
            {{std::string("2020-02-29 00:00:00")}, 29}
    };

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, day_of_year_test) {
    std::string func_name = "dayofyear";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::DateTime};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("2020-00-01 00:00:00")}, Null()},
            {{std::string("2020-01-00 00:00:00")}, Null()},
            {{std::string("2020-02-29 00:00:00")}, 60}
    };

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, week_of_year_test) {
    std::string func_name = "weekofyear";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::DateTime};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("2020-00-01 00:00:00")}, Null()},
            {{std::string("2020-01-00 00:00:00")}, Null()},
            {{std::string("2020-02-29 00:00:00")}, 9}
    };

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, year_test) {
    std::string func_name = "year";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::DateTime};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("2021-01-01 00:00:00")}, 2021},
            {{std::string("2021-01-00 00:00:00")}, Null()},
            {{std::string("2025-05-01 00:00:00")}, 2025}
    };

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, unix_timestamp_test) {
    std::string func_name = "unix_timestamp";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::DateTime};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("9999-12-30 00:00:00")}, 0},
            {{std::string("1000-01-01 00:00:00")}, 0}
    };

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, from_unix_test) {
    std::string func_name = "from_unixtime";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::Int32};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{1565080737}, std::string("2019-08-06 16:38:57")},
            {{-123}, Null()}
    };

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, timediff_test) {
    std::string func_name = "timediff";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::DateTime, vectorized::TypeIndex::DateTime};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 12:00:00")}, 0.0},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 13:01:02")}, -3662.0},
            {{std::string("2019-00-18 12:00:00"), std::string("2019-07-18 13:01:02")}, Null()},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-00 13:01:02")}, Null()}
    };

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
