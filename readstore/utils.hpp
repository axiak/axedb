#pragma once
#ifndef __DULLAHAN_READSTORE_UTILS_H_
#define __DULLAHAN_READSTORE_UTILS_H_

#include <vector>
#include <type_traits>
#include <limits.h>

namespace dullahan {
namespace append_numbers {


/**
* Append an arbitrary numeric type rvalue to a vector with
* arbitrary byte width.
*
* For example:
*
* std::vector<char> vector;
* uint32_t value = 123456;
* append_numbers::addBytes(&vector, value);
*
* would append {\x40, \xe2, \x01, \x00}
*/
template <typename T>
struct BitInfo {
  constexpr static size_t NUM_BITS = sizeof(T) * CHAR_BIT;
  constexpr static unsigned long long LOWER_MASK = (2ULL << NUM_BITS) - 1;
};

template <typename SourceType, typename TargetType>
struct VectorSizeInfo {
  constexpr static size_t vectorSize() {
    return sizeof(SourceType) / sizeof(TargetType);
  }
};

template <int N, typename SourceType, typename TargetType>
struct Addbytes {
  static inline void run(std::vector<TargetType>* target, SourceType remaining) {
    static_assert(N > 0, "Cannot run AddBytes with fewer than 1 byte.");
    target->push_back(remaining & BitInfo<TargetType>::LOWER_MASK);
    remaining >>= BitInfo<TargetType>::NUM_BITS;
    Addbytes<N - sizeof(TargetType), SourceType, TargetType>::run(target, remaining);
  }
};


template <typename SourceType, typename TargetType>
struct Addbytes<1, SourceType, TargetType> {
  static inline void run(std::vector<TargetType>* target, SourceType remaining) {
    target->push_back(remaining);
  }
};

template <typename SourceType, typename TargetType>
void inline addBytes(std::vector<TargetType>* target, SourceType input) {
  target->reserve(target->size() + VectorSizeInfo<SourceType, TargetType>::vectorSize());
  Addbytes<sizeof(SourceType), SourceType, TargetType>::run(target, input);
}


}
}

#endif // __DULLAHAN_READSTORE_UTILS_H_