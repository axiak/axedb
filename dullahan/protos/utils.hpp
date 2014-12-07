#pragma once
#ifndef __DULLAHAN_UTILS_HPP_
#define __DULLAHAN_UTILS_HPP_

#include <google/protobuf/message_lite.h>
#include <rocksdb/slice.h>
#include <exception>
#include <string>

#include "dullahan.pb.h"

namespace dullahan {

namespace models {

/**
* Serialize a protobuf to a slice, using the provided buffer as
*/
::rocksdb::Slice toSlice(const ::google::protobuf::MessageLite & message, std::string * buffer) {
  buffer->resize(0);

  if (!message.SerializeToString(buffer)) {
    throw std::runtime_error{"Unable to serialize protobuf."};
  };

  return ::rocksdb::Slice{*buffer};
}

/**
* Deserialize a protobuf from a slice
*/
template<typename T>
T fromSlice(const ::rocksdb::Slice & slice) {
  T item;
  item.ParseFromString(slice.data(), slice.size());
  return item;
}


} // namespace models

bool isBigEndian(void)
{
  union {
    uint32_t i;
    char c[4];
  } buffer = {0x01020304};

  return buffer.c[0] == 1;
}

} // namespace dullahan

#endif // __DULLAHAN_UTILS_HPP_