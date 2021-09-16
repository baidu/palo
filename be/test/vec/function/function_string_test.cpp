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
#include "vec/functions/function_string.h"

#include <gtest/gtest.h>
#include <time.h>

#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "function_test_util.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/url_coding.h"
#include "vec/core/field.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
using vectorized::Null;

TEST(function_string_test, function_string_substr_test) {
    std::string func_name = "substr";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::Int32,
                                                      vectorized::TypeIndex::Int32};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("asd你好"), 4, 10}, std::string("\xE4\xBD\xA0\xE5\xA5\xBD")}, //你好
            {{std::string("hello word"), -5, 5}, std::string(" word")},
            {{std::string("hello word"), 1, 12}, std::string("hello word")},
            {{std::string("HELLO,!^%"), 4, 2}, std::string("LO")},
            {{std::string(""), 5, 4}, Null()},
            {{Null(), 5, 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_strright_test) {
    std::string func_name = "strright";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::Int32};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("asd"), 1}, std::string("d")},
            {{std::string("hello word"), -2}, std::string("ello word")},
            {{std::string("hello word"), 20}, std::string("hello word")},
            {{std::string("HELLO,!^%"), 2}, std::string("^%")},
            {{std::string(""), 3}, std::string("")},
            {{Null(), 3}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_strleft_test) {
    std::string func_name = "strleft";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::Int32};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("asd"), 1}, std::string("a")},
            {{std::string("hel  lo  "), 5}, std::string("hel  ")},
            {{std::string("hello word"), 20}, std::string("hello word")},
            {{std::string("HELLO,!^%"), 7}, std::string("HELLO,!")},
            {{std::string(""), 2}, Null()},
            {{Null(), 3}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_lower_test) {
    std::string func_name = "lower";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};
    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("ASD")}, std::string("asd")},
            {{std::string("HELLO123")}, std::string("hello123")},
            {{std::string("MYtestSTR")}, std::string("myteststr")},
            {{std::string("HELLO,!^%")}, std::string("hello,!^%")},
            {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_upper_test) {
    std::string func_name = "upper";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};
    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("asd")}, std::string("ASD")},
            {{std::string("hello123")}, std::string("HELLO123")},
            {{std::string("HELLO,!^%")}, std::string("HELLO,!^%")},
            {{std::string("MYtestStr")}, std::string("MYTESTSTR")},
            {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}
TEST(function_string_test, function_string_trim_test) {
    std::string func_name = "trim";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};
    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("a sd")}, std::string("a sd")},
            {{std::string("  hello 123  ")}, std::string("hello 123")},
            {{std::string("  HELLO,!^%")}, std::string("HELLO,!^%")},
            {{std::string("MY test Str你好  ")}, std::string("MY test Str你好")},
            {{Null()}, Null()},
            {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_ltrim_test) {
    std::string func_name = "ltrim";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};
    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("a sd")}, std::string("a sd")},
            {{std::string("  hello 123  ")}, std::string("hello 123  ")},
            {{std::string("  HELLO,!^%")}, std::string("HELLO,!^%")},
            {{std::string("  MY test Str你好  ")}, std::string("MY test Str你好  ")},
            {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_rtrim_test) {
    std::string func_name = "rtrim";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};
    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("a sd ")}, std::string("a sd")},
            {{std::string("  hello 123  ")}, std::string("  hello 123")},
            {{std::string("  HELLO,!^%")}, std::string("  HELLO,!^%")},
            {{std::string("  MY test Str你好  ")}, std::string("  MY test Str你好")},
            {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}
TEST(function_string_test, function_string_repeat_test) {
    std::string func_name = "repeat";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::Int32};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("a"), 3}, std::string("aaa")},
            {{std::string("hel lo"), 2}, std::string("hel lohel lo")},
            {{std::string("hello word"), -1}, std::string("")},
            {{std::string(""), 1}, std::string("")},
            {{std::string("HELLO,!^%"), 2}, std::string("HELLO,!^%HELLO,!^%")},
            {{std::string("你"), 2}, std::string("你你")}};
    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_reverse_test) {
    std::string func_name = "reverse";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};
    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("asd ")}, std::string(" dsa")},
            {{std::string("  hello 123  ")}, std::string("  321 olleh  ")},
            {{std::string("  HELLO,!^%")}, std::string("%^!,OLLEH  ")},
            {{std::string("你好啊")}, std::string("啊好你")},
            {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_length_test) {
    std::string func_name = "length";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};
    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("asd ")}, int32_t(4)},
            {{std::string("  hello 123  ")}, int32_t(13)},
            {{std::string("  HELLO,!^%")}, int32_t(11)},
            {{std::string("你好啊")}, int32_t(9)},
            {{std::string("")}, int32_t(0)}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_append_trailing_char_if_absent_test) {
    std::string func_name = "append_trailing_char_if_absent";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("ASD"), std::string("D")}, std::string("ASD")},
            {{std::string("AS"), std::string("D")}, std::string("ASD")},
            {{std::string(""), std::string("")}, Null()},
            {{std::string(""), std::string("A")}, std::string("A")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_starts_with_test) {
    std::string func_name = "starts_with";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("hello world"), std::string("hello")}, uint8_t(1)},
            {{std::string("hello world"), std::string("world")}, uint8_t(0)},
            {{std::string("你好"), std::string("你")}, uint8_t(1)},
            {{std::string(""), std::string("")}, uint8_t(1)},
            {{std::string("你好"), Null()}, Null()},
            {{Null(), std::string("")}, Null()}};

    vectorized::check_function<vectorized::DataTypeUInt8, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_ends_with_test) {
    std::string func_name = "ends_with";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("hello world"), std::string("hello")}, uint8_t(0)},
            {{std::string("hello world"), std::string("world")}, uint8_t(1)},
            {{std::string("你好"), std::string("好")}, uint8_t(1)},
            {{std::string(""), std::string("")}, uint8_t(1)},
            {{std::string("你好"), Null()}, Null()},
            {{Null(), std::string("")}, Null()}};

    vectorized::check_function<vectorized::DataTypeUInt8, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_lpad_test) {
    std::string func_name = "lpad";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::Int32,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("hi"), 5, std::string("?")}, std::string("???hi")},
            {{std::string("g8%7IgY%AHx7luNtf8Kh"), 20, std::string("")},
             std::string("g8%7IgY%AHx7luNtf8Kh")},
            {{std::string("hi"), 1, std::string("?")}, std::string("h")},
            {{std::string("你好"), 1, std::string("?")}, std::string("你")},
            {{std::string("hi"), 0, std::string("?")}, std::string("")},
            {{std::string("hi"), -1, std::string("?")}, Null()},
            {{std::string("h"), 1, std::string("")}, std::string("h")},
            {{std::string("hi"), 5, std::string("")}, Null()},
            {{std::string("hi"), 5, std::string("ab")}, std::string("abahi")},
            {{std::string("hi"), 5, std::string("呵呵")}, std::string("呵呵呵hi")},
            {{std::string("呵呵"), 5, std::string("hi")}, std::string("hih呵呵")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_rpad_test) {
    std::string func_name = "rpad";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::Int32,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("hi"), 5, std::string("?")}, std::string("hi???")},
            {{std::string("g8%7IgY%AHx7luNtf8Kh"), 20, std::string("")},
             std::string("g8%7IgY%AHx7luNtf8Kh")},
            {{std::string("hi"), 1, std::string("?")}, std::string("h")},
            {{std::string("你好"), 1, std::string("?")}, std::string("你")},
            {{std::string("hi"), 0, std::string("?")}, std::string("")},
            {{std::string("hi"), -1, std::string("?")}, Null()},
            {{std::string("h"), 1, std::string("")}, std::string("h")},
            {{std::string("hi"), 5, std::string("")}, Null()},
            {{std::string("hi"), 5, std::string("ab")}, std::string("hiaba")},
            {{std::string("hi"), 5, std::string("呵呵")}, std::string("hi呵呵呵")},
            {{std::string("呵呵"), 5, std::string("hi")}, std::string("呵呵hih")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_ascii_test) {
    std::string func_name = "ascii";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {{{std::string("")}, 0},
                                                                        {{std::string("aa")}, 97},
                                                                        {{std::string("我")}, 230},
                                                                        {{Null()}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_char_length_test) {
    std::string func_name = "char_length";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("")}, 0},    {{std::string("aa")}, 2},  {{std::string("我")}, 1},
            {{std::string("我a")}, 2}, {{std::string("a我")}, 2}, {{std::string("123")}, 3},
            {{Null()}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_concat_test) {
    std::string func_name = "concat";
    {
        std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};

        std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
                {{std::string("")}, std::string("")},
                {{std::string("123")}, std::string("123")},
                {{Null()}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };

    {
        std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                          vectorized::TypeIndex::String};

        std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
                {{std::string(""), std::string("")}, std::string("")},
                {{std::string("123"), std::string("45")}, std::string("12345")},
                {{std::string("123"), Null()}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };

    {
        std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                          vectorized::TypeIndex::String,
                                                          vectorized::TypeIndex::String};

        std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
                {{std::string(""), std::string("1"), std::string("")}, std::string("1")},
                {{std::string("123"), std::string("456"), std::string("789")},
                 std::string("123456789")},
                {{std::string("123"), Null(), std::string("789")}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };
}

TEST(function_string_test, function_concat_ws_test) {
    std::string func_name = "concat_ws";
    {
        std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                          vectorized::TypeIndex::String};

        std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
                {{std::string("-"), std::string("")}, std::string("")},
                {{std::string(""), std::string("123")}, std::string("123")},
                {{std::string(""), std::string("")}, std::string("")},
                {{Null(), std::string("")}, Null()},
                {{Null(), Null()}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };

    {
        std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                          vectorized::TypeIndex::String,
                                                          vectorized::TypeIndex::String};

        std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
                {{std::string("-"), std::string(""), std::string("")}, std::string("-")},
                {{std::string(""), std::string("123"), std::string("456")}, std::string("123456")},
                {{std::string(""), std::string(""), std::string("")}, std::string("")},
                {{Null(), std::string(""), std::string("")}, Null()},
                {{Null(), std::string(""), Null()}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };

    {
        std::vector<vectorized::TypeIndex> input_types = {
                vectorized::TypeIndex::String, vectorized::TypeIndex::String,
                vectorized::TypeIndex::String, vectorized::TypeIndex::String};

        std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
                {{std::string("-"), std::string(""), std::string(""), std::string("")},
                 std::string("--")},
                {{std::string(""), std::string("123"), std::string("456"), std::string("789")},
                 std::string("123456789")},
                {{std::string("-"), std::string(""), std::string("?"), std::string("")},
                 std::string("-?-")},
                {{Null(), std::string(""), std::string("?"), std::string("")}, Null()},
                {{std::string("-"), std::string("123"), Null(), std::string("456")},
                 std::string("123-456")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };
}

TEST(function_string_test, function_null_or_empty_test) {
    std::string func_name = "null_or_empty";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("")}, uint8(true)},
            {{std::string("aa")}, uint8(false)},
            {{std::string("我")}, uint8(false)},
            {{Null()}, uint8(true)}};

    vectorized::check_function<vectorized::DataTypeUInt8, false>(func_name, input_types, data_set);
}

TEST(function_string_test, function_to_base64_test) {
    std::string func_name = "to_base64";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("asd你好")}, {std::string("YXNk5L2g5aW9")}},
            {{std::string("hello world")}, {std::string("aGVsbG8gd29ybGQ=")}},
            {{std::string("HELLO,!^%")}, {std::string("SEVMTE8sIV4l")}},
            {{std::string("")}, {Null()}},
            {{std::string("MYtestSTR")}, {std::string("TVl0ZXN0U1RS")}},
            {{std::string("ò&ø")}, {std::string("w7Imw7g=")}}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_from_base64_test) {
    std::string func_name = "from_base64";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("YXNk5L2g5aW9")}, {std::string("asd你好")}},
            {{std::string("aGVsbG8gd29ybGQ=")}, {std::string("hello world")}},
            {{std::string("SEVMTE8sIV4l")}, {std::string("HELLO,!^%")}},
            {{std::string("")}, {Null()}},
            {{std::string("TVl0ZXN0U1RS")}, {std::string("MYtestSTR")}},
            {{std::string("w7Imw7g=")}, {std::string("ò&ø")}},
            {{std::string("ò&ø")}, {Null()}},
            {{std::string("你好哈喽")}, {Null()}}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_reverse_test) {
    std::string func_name = "reverse";
    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String};
    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("")}, {std::string("")}},
            {{std::string("a")}, {std::string("a")}},
            {{std::string("美团和和阿斯顿百度ab")}, {std::string("ba度百顿斯阿和和团美")}},
            {{std::string("!^%")}, {std::string("%^!")}},
            {{std::string("ò&ø")}, {std::string("ø&ò")}},
            {{std::string("A攀c")}, {std::string("c攀A")}},
            {{std::string("NULL")}, {std::string("LLUN")}}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_instr_test) {
    std::string func_name = "instr";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("abcdefg"), std::string("efg")}, 5},
            {{std::string("aa"), std::string("a")}, 1},
            {{std::string("我是"), std::string("是")}, 2},
            {{std::string("abcd"), std::string("e")}, 0},
            {{std::string("abcdef"), std::string("")}, 1},
            {{std::string(""), std::string("")}, 1},
            {{std::string("aaaab"), std::string("bb")}, 0}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_find_in_set_test) {
    std::string func_name = "find_in_set";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::any>> data_set = {
            {{std::string("abcdefg"), std::string("a,b,c")}, 0},
            {{std::string("aa"), std::string("a,aa,aaa")}, 2},
            {{std::string("aa"), std::string("aa,aa,aa")}, 1},
            {{std::string("a"), Null()}, Null()},
            {{Null(), std::string("aa")}, Null()},
            {{std::string("a"), std::string("")}, 0},
            {{std::string(""), std::string(",,")}, 1}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
