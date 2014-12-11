#include <rocksdb/env.h>
#include <sstream>
#include <iostream>

#include "../utils/utils.hpp"
#include "./models/value.hpp"
#include "ewahmerge.hpp"

namespace {

inline uint64_t readUInt64(const rocksdb::Slice * const slice, const size_t offset) {
  return *reinterpret_cast<const uint64_t *>(slice->data() + offset);
}

constexpr size_t WORD_SIZE = sizeof(dullahan::models::bitarrayword);

} // anonymous namespace

namespace dullahan {

/**
* Merge two EWAH bitmasks assuming that they are already
* padded to word boundaries - concatenate except adding the last two sets of 64-bit longs.
*/
bool EWAHMergeOperator::Merge(
    const rocksdb::Slice &key,
    const rocksdb::Slice *existing_value,
    const rocksdb::Slice &value,
    std::string *new_value,
    rocksdb::Logger *logger) const {
  assert(new_value);
  new_value->clear();

  if (existing_value) {
    new_value->append(existing_value->data(), existing_value->size());
    EWAHBoolArray<dullahan::models::bitarrayword>::concatStreams(new_value, value.data(), value.size());
  }
  else {
    new_value->append(value.data(), value.size());
  }

  return true;
}

}
