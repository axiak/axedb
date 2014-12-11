#pragma once
#ifndef __DULLAHAN_READSTORE_KEY_H_
#define __DULLAHAN_READSTORE_KEY_H_

#include <vector>
#include <rocksdb/slice.h>
#include <array>
#include <limits.h>
#include <MurmurHash3.h>


namespace dullahan {


using column_t = std::uint16_t;
using byte = char;
const column_t META_COLUMN = -1;
const column_t MATERIALIZED_ROW_COLUMN = -2;

namespace models {

class ReadStoreKey {
public:

  template<typename T1>
  ReadStoreKey(T1 &&column, const std::string &columnData) : bytes_{} {
    column_ = std::forward<T1>(column);
    bytes_.reserve(columnData.size());
    bytes_.insert(bytes_.end(), columnData.begin(), columnData.end());
    updateSlice();
  }

  template<typename T1, typename T2>
  ReadStoreKey(T1 &&column, T2 &&bytes) {
    column_ = std::forward<T1>(column);
    bytes_ = std::forward<T2>(bytes);
    updateSlice();
  }

  ReadStoreKey();

  ~ReadStoreKey() = default;

  ReadStoreKey(const ReadStoreKey &);

  ReadStoreKey &operator=(const ReadStoreKey &);

  ReadStoreKey(ReadStoreKey &&);

  ReadStoreKey &operator=(ReadStoreKey &&);

  size_t columnSize() const;

  enum KeyType {
    META,
    MATERIALIZED_ROW,
    COLUMN
  };

  inline KeyType getKeyType() const {
    switch (column_) {
      case MATERIALIZED_ROW_COLUMN:
        return KeyType::MATERIALIZED_ROW;
      case META_COLUMN:
        return KeyType::META;
      default:
        return KeyType::COLUMN;
    }
  }

  column_t column() const;

  rocksdb::Slice toSlice() const;

  struct Comparator_ {
    bool operator()(const ReadStoreKey &a, const ReadStoreKey &b) const {
      if (a.column_ < b.column_) {
        return true;
      }
      {
        unsigned long max_i = std::min(a.bytes_.size(), b.bytes_.size());
        for (size_t i = 0; i < max_i; ++i) {
          if (a.bytes_[i] < b.bytes_[i]) {
            return true;
          }
        }
      }
      return a.bytes_.size() < b.bytes_.size();
    }
  };

  static const Comparator_ comparator() {
    static Comparator_ comparator;
    return comparator;
  }

  struct Equality_ {
    bool operator()(const ReadStoreKey &a, const ReadStoreKey &b) const {
      if (a.column_ != b.column_) {
        return false;
      }
      {
        unsigned long max_i = std::min(a.bytes_.size(), b.bytes_.size());
        for (size_t i = 0; i < max_i; ++i) {
          if (a.bytes_[i] != b.bytes_[i]) {
            return false;
          }
        }
      }
      return a.bytes_.size() == b.bytes_.size();
    }
  };

  static const Equality_ equality() {
    static Equality_ equality;
    return equality;
  }

  struct Hasher_ {
    constexpr static uint32_t seed = 796788753;

    size_t operator()(const ReadStoreKey &a) const {
      uint32_t output;
      MurmurHash3_x86_32(
          reinterpret_cast<const void *>(a.bytes_.data()),
          sizeof(byte) / sizeof(void *),
          seed,
          &output
      );
      return 31 * a.column_ + output;
    }
  };

  static const Hasher_ hasher() {
    static Hasher_ hasher;
    return hasher;
  }


  static ReadStoreKey metadataKey() {
    ReadStoreKey metadataKey(META_COLUMN, {});
    return metadataKey;
  }

  static ReadStoreKey materializedRowKey(long rowId) {
    std::vector<byte> vector;
    const byte *ptr = reinterpret_cast<const byte *>(&rowId);
    vector.insert(vector.end(), ptr, ptr + sizeof(rowId));
    ReadStoreKey rowKey(MATERIALIZED_ROW_COLUMN, std::move(vector));
    return rowKey;
  }

  static bool isMaterializedRowKey(const rocksdb::Slice & slice) {
    return (slice.size() > sizeof(column_t) &&
        *reinterpret_cast<const column_t *>(slice.data()) == MATERIALIZED_ROW_COLUMN);
  }

private:
  column_t column_;
  std::vector<byte> bytes_;
  std::vector<byte> slice_repr_;

  void updateSlice();
};


}
}


#endif // __DULLAHAN_READSTORE_KEY_H_
