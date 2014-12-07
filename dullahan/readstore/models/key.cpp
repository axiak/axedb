#include <rocksdb/slice.h>
#include <limits>
#include <iostream>

#include <vector>
#include "key.hpp"

namespace dullahan {

namespace models {

ReadStoreKey::ReadStoreKey() : column_(0), bytes_(), slice_repr_() {
  updateSlice();
}

ReadStoreKey::ReadStoreKey(ReadStoreKey const &aConst) :
    column_(aConst.column_),
    bytes_(aConst.bytes_) {
  updateSlice();
}

ReadStoreKey &ReadStoreKey::operator=(ReadStoreKey const &aConst) {
  column_ = aConst.column_;
  bytes_ = aConst.bytes_;
  updateSlice();
  return *this;
}

ReadStoreKey::ReadStoreKey(ReadStoreKey &&key) {
  column_ = key.column_;
  bytes_ = std::move(key.bytes_);
  slice_repr_ = std::move(key.slice_repr_);
}

ReadStoreKey &ReadStoreKey::operator=(ReadStoreKey &&key) {
  column_ = key.column_;
  bytes_ = std::move(key.bytes_);
  slice_repr_ = std::move(key.slice_repr_);
  return *this;
}


size_t ReadStoreKey::columnSize() const {
  return bytes_.size();
}

column_t ReadStoreKey::column() const {
  return column_;
}

void ReadStoreKey::updateSlice() {
  slice_repr_.clear();
  slice_repr_.reserve(sizeof(column_) + bytes_.size());
  const byte * ptr = reinterpret_cast<const byte *>(&column_);
  slice_repr_.insert(slice_repr_.end(), ptr, ptr + sizeof(column_));
  slice_repr_.insert(slice_repr_.end(), bytes_.begin(), bytes_.end());
}

rocksdb::Slice ReadStoreKey::toSlice() const {
  return rocksdb::Slice(slice_repr_.data(), slice_repr_.size());
}

}
}