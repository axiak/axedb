#pragma once
#ifndef __DULLAHAN_UTILS_BYTES_HPP_
#define __DULLAHAN_UTILS_BYTES_HPP_

#include <stdint.h>

namespace dullahan {
namespace bytes {

constexpr uint64_t operator"" _B( unsigned long long bytes) {
  return static_cast<uint64_t>(bytes);
}

constexpr uint64_t operator"" _KB(unsigned long long bytes) {
  return static_cast<uint64_t>(bytes << 10);
}

constexpr uint64_t operator"" _MB(unsigned long long bytes) {
  return static_cast<uint64_t>(bytes << 20);
}

constexpr uint64_t operator"" _GB(unsigned long long bytes) {
  return static_cast<uint64_t>(bytes << 30);
}


}
}




#endif // __DULLAHAN_UTILS_BYTES_HPP_