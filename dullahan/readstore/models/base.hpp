#pragma once
#ifndef __DULLAHAN_MODELS_BASE_HPP_
#define __DULLAHAN_MODELS_BASE_HPP_

namespace dullahan {
namespace models {

using bitarrayword = uint64_t;

constexpr size_t kSizeOfBitArrayBits = sizeof(bitarrayword) * 8;
constexpr size_t kSizeOfBitArrayBytes = sizeof(bitarrayword);


}
}

#endif // __DULLAHAN_MODELS_BASE_HPP_