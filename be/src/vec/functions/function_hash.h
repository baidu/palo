#pragma once
#include <utility>

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/bit_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

struct IntHash32Impl {
    using ReturnType = UInt32;

    static UInt32 apply(UInt64 x) {
        /// seed is taken from /dev/urandom. It allows you to avoid undesirable dependencies with hashes in different data structures.
        return int_hash32<0x75D9543DE018BF45ULL>(x);
    }
};

struct IntHash64Impl {
    using ReturnType = UInt64;

    static UInt64 apply(UInt64 x) { return int_hash64(x ^ 0x4CF2D2BAAE6DA887ULL); }
};

template <typename Impl>
class FunctionAnyHash : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionAnyHash>(); }

private:
    using ToType = typename Impl::ReturnType;

    template <typename FromType, bool first>
    Status execute_int_type(const IColumn* column,
                            typename ColumnVector<ToType>::Container& vec_to) {
        if (const ColumnVector<FromType>* col_from =
                    check_and_get_column<ColumnVector<FromType>>(column)) {
            const typename ColumnVector<FromType>::Container& vec_from = col_from->get_data();
            size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i) {
                ToType h;

                if constexpr (Impl::use_int_hash_for_pods) {
                    if constexpr (std::is_same_v<ToType, UInt64>)
                        h = IntHash64Impl::apply(ext::bit_cast<UInt64>(vec_from[i]));
                    else
                        h = IntHash32Impl::apply(ext::bit_cast<UInt32>(vec_from[i]));
                } else {
                    h = Impl::apply(reinterpret_cast<const char*>(&vec_from[i]),
                                    sizeof(vec_from[i]));
                }

                if (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::combineHashes(vec_to[i], h);
            }
        } else if (auto col_from_const =
                           check_and_get_column_const<ColumnVector<FromType>>(column)) {
            auto value = col_from_const->template get_value<FromType>();
            ToType hash;
            if constexpr (std::is_same_v<ToType, UInt64>)
                hash = IntHash64Impl::apply(ext::bit_cast<UInt64>(value));
            else
                hash = IntHash32Impl::apply(ext::bit_cast<UInt32>(value));

            size_t size = vec_to.size();
            if (first) {
                vec_to.assign(size, hash);
            } else {
                for (size_t i = 0; i < size; ++i) vec_to[i] = Impl::combineHashes(vec_to[i], hash);
            }
        } else {
            LOG(FATAL) << fmt::format("Illegal column {} of argument of function {}",
                                      column->get_name(), get_name());
        }
        return Status::OK();
    }

    template <bool first>
    Status execute_string(const IColumn* column, typename ColumnVector<ToType>::Container& vec_to) {
        if (const ColumnString* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                const ToType h = Impl::apply(reinterpret_cast<const char*>(&data[current_offset]),
                                             offsets[i] - current_offset - 1);

                if (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::combineHashes(vec_to[i], h);

                current_offset = offsets[i];
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            String value = col_from_const->get_value<String>().data();
            const ToType hash = Impl::apply(value.data(), value.size());
            const size_t size = vec_to.size();

            if (first) {
                vec_to.assign(size, hash);
            } else {
                for (size_t i = 0; i < size; ++i) {
                    vec_to[i] = Impl::combineHashes(vec_to[i], hash);
                }
            }
        } else {
            LOG(FATAL) << fmt::format("Illegal column {} of first argument of function ",
                                      column->get_name(), get_name());
        }
        return Status::OK();
    }

    template <bool first>
    Status execute_any(const IDataType* from_type, const IColumn* icolumn,
                       typename ColumnVector<ToType>::Container& vec_to) {
        WhichDataType which(from_type);

        if (which.is_uint8())
            execute_int_type<UInt8, first>(icolumn, vec_to);
        else if (which.is_int16())
            execute_int_type<UInt16, first>(icolumn, vec_to);
        else if (which.is_uint32())
            execute_int_type<UInt32, first>(icolumn, vec_to);
        else if (which.is_uint64())
            execute_int_type<UInt64, first>(icolumn, vec_to);
        else if (which.is_int8())
            execute_int_type<Int8, first>(icolumn, vec_to);
        else if (which.is_int16())
            execute_int_type<Int16, first>(icolumn, vec_to);
        else if (which.is_int32())
            execute_int_type<Int32, first>(icolumn, vec_to);
        else if (which.is_int64())
            execute_int_type<Int64, first>(icolumn, vec_to);
        else if (which.is_date_or_datetime()) {
            // TODO: handle Date
            LOG(FATAL) << "need implemnt here";
        } else if (which.is_float32())
            execute_int_type<Float32, first>(icolumn, vec_to);
        else if (which.is_float64())
            execute_int_type<Float64, first>(icolumn, vec_to);
        else if (which.is_string())
            execute_string<first>(icolumn, vec_to);
        else {
            LOG(FATAL) << fmt::format("Unexpected type {} of argument of function {}",
                                      from_type->get_name(), get_name());
        }
        return Status::OK();
    }

    Status execute_for_argument(const IDataType* type, const IColumn* column,
                                typename ColumnVector<ToType>::Container& vec_to, bool& is_first) {
        if (is_first)
            execute_any<true>(type, column, vec_to);
        else
            execute_any<false>(type, column, vec_to);

        is_first = false;
        return Status::OK();
    }

public:
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    bool use_default_implementation_for_constants() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeNumber<ToType>>();
    }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        size_t rows = input_rows_count;
        auto col_to = ColumnVector<ToType>::create(rows);

        typename ColumnVector<ToType>::Container& vec_to = col_to->get_data();

        if (arguments.empty()) {
            /// Constant random number from /dev/urandom is used as a hash value of empty list of arguments.
            vec_to.assign(rows, static_cast<ToType>(0xe28dbde7fe22e41c));
        }

        /// The function supports arbitrary number of arguments of arbitrary types.

        bool is_first_argument = true;
        for (size_t i = 0; i < arguments.size(); ++i) {
            const ColumnWithTypeAndName& col = block.get_by_position(arguments[i]);
            execute_for_argument(col.type.get(), col.column.get(), vec_to, is_first_argument);
        }

        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};
} // namespace doris::vectorized
