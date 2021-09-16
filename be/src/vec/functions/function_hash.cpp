#include "vec/functions/function_hash.h"

#include "util/hash_util.hpp"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
struct MurmurHash2Impl64 {
    static constexpr auto name = "murmurHash2_64";
    using ReturnType = UInt64;

    static UInt64 apply(const char* data, const size_t size) {
        return HashUtil::murmur_hash2_64(data, size, 0);
    }

    static UInt64 combineHashes(UInt64 h1, UInt64 h2) { return IntHash64Impl::apply(h1) ^ h2; }

    static constexpr bool use_int_hash_for_pods = false;
};
using FunctionMurmurHash2_64 = FunctionAnyHash<MurmurHash2Impl64>;

void register_function_function_hash(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMurmurHash2_64>();
}
} // namespace doris::vectorized