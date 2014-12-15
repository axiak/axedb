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
const column_t kMaterializedRowColumn = static_cast<column_t>(-1);
const column_t kAllRowsColumn = static_cast<column_t>(-2);
const column_t kAllRowsForColumn = static_cast<column_t>(-3);

namespace models {

class ReadStoreKey {
public:
  ReadStoreKey();

  ~ReadStoreKey() = default;

  ReadStoreKey(const ReadStoreKey &);

  ReadStoreKey &operator=(const ReadStoreKey &);

  ReadStoreKey(ReadStoreKey &&) noexcept;

  ReadStoreKey &operator=(ReadStoreKey &&) noexcept;


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


  size_t column_size() const;

  enum KeyType {
    MATERIALIZED_ROW,
    COLUMN
  };

  inline KeyType getKeyType() const {
    switch (column_) {
      case kMaterializedRowColumn:
        return KeyType::MATERIALIZED_ROW;
      default:
        return KeyType::COLUMN;
    }
  }

  column_t column() const;

  rocksdb::Slice ToSlice() const;

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

  static ReadStoreKey AllRowsKey() {
    static std::vector<byte> vector{};
    static ReadStoreKey row_key{kAllRowsColumn, vector};
    return row_key;
  }

  static ReadStoreKey MaterializedRowKey(uint32_t row_id) {
    std::vector<byte> vector;
    const byte *ptr = reinterpret_cast<const byte *>(&row_id);
    vector.insert(vector.end(), ptr, ptr + sizeof(uint32_t));
    ReadStoreKey row_key(kMaterializedRowColumn, std::move(vector));
    return row_key;
  }

  static ReadStoreKey AllRowsForColumnKey(column_t column) {
    std::vector<byte> vector;
    const byte *ptr = reinterpret_cast<const byte *>(&column);
    vector.insert(vector.end(), ptr, ptr + sizeof(column_t));
    ReadStoreKey row_key{kAllRowsForColumn, std::move(vector)};
    return row_key;
  }

  static ReadStoreKey EmptyColumnKey(column_t column) {
    std::vector<byte> vector{};
    ReadStoreKey row_key(column, std::move(vector));
    return row_key;
  }

  static bool IsMaterializedRowKey(const rocksdb::Slice & slice) {
    return (slice.size() > sizeof(column_t) &&
        *reinterpret_cast<const column_t *>(slice.data()) == kMaterializedRowColumn);
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
