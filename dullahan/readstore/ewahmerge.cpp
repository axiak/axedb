#include <rocksdb/env.h>
#include <sstream>

#include "ewahmerge.hpp"

namespace dullahan {


inline uint64_t readUInt64(const rocksdb::Slice * const slice, const size_t offset) {
  return *reinterpret_cast<const uint64_t *>(slice->data() + offset);
}

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

    constexpr size_t singleOffset = sizeof(uint64_t);

    const uint64_t bufferSize = readUInt64(existing_value, existing_value->size() - 2 * singleOffset) +
        readUInt64(&value, value.size() - 2 * singleOffset);

    const uint64_t sizeInBits = readUInt64(existing_value, existing_value->size() - singleOffset) +
        readUInt64(&value, value.size() - singleOffset);

    new_value->resize(2 * singleOffset + bufferSize);

    new_value->append(existing_value->data(), existing_value->size() - 2 * singleOffset);
    new_value->append(value.data(), value.size() - 2 * singleOffset);
    new_value->append(reinterpret_cast<const char *> (&bufferSize),
        sizeof(bufferSize));
    new_value->append(reinterpret_cast<const char *> (&sizeInBits),
        sizeof(sizeInBits));
  }
  else {
    new_value->append(value.data(), value.size());
  }

  return true;
}

}
