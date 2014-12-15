#pragma once
#ifndef __DULLAHAN_UTILS_HPP_
#define __DULLAHAN_UTILS_HPP_

#include <google/protobuf/message_lite.h>
#include <rocksdb/slice.h>
#include <exception>
#include <string>
#include <stdint.h>

#include "dullahan.pb.h"

namespace dullahan {

bool IsBigEndian(void);

namespace models {

/**
* Serialize a protobuf to a slice, using the provided buffer as
*/
::rocksdb::Slice ToSlice(const ::google::protobuf::MessageLite & message, std::string * buffer);

/**
* Deserialize a protobuf from a slice
*/
template<typename T>
T fromSlice(const ::rocksdb::Slice & slice);

TabletMetadata_Endianness CurrentEndianness();

} // namespace models
} // namespace dullahan

#endif // __DULLAHAN_UTILS_HPP_