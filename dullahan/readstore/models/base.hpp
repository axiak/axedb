#pragma once
#ifndef __DULLAHAN_MODELS_BASE_HPP_
#define __DULLAHAN_MODELS_BASE_HPP_

#include <stdlib.h>
#include <stdint.h>
#include <ewah.h>

namespace dullahan {
namespace models {

using bitarrayword = uint64_t;

using BitArray = EWAHBoolArray<bitarrayword>;

constexpr size_t kSizeOfBitArrayBits = sizeof(bitarrayword) * 8;
constexpr size_t kSizeOfBitArrayBytes = sizeof(bitarrayword);


}
}

#endif // __DULLAHAN_MODELS_BASE_HPP_