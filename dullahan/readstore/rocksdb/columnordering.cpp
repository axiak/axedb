#include <rocksdb/comparator.h>
#include <rocksdb/slice.h>

#include "columnordering.hpp"
#include "../models/key.hpp"

namespace {
constexpr int kSizeOfColumn = sizeof(dullahan::column_t);

template<typename T>
int CompareNumericType(const rocksdb::Comparator *fallback, const rocksdb::Slice &a, const rocksdb::Slice &b) {
  constexpr size_t minimum_size = sizeof(T) + kSizeOfColumn;
  if (a.size() < minimum_size || b.size() < minimum_size) {
    return fallback->Compare(a, b);
  }

  const T *a_val, *b_val;

  a_val = reinterpret_cast<const T *>(a.data() + kSizeOfColumn);
  b_val = reinterpret_cast<const T *>(b.data() + kSizeOfColumn);

  if (*a_val < *b_val) {
    return -1;
  } else if (*a_val > *b_val) {
    return 1;
  } else {
    return 0;
  }
}
} // anonymous namespace

namespace dullahan {



DullahanReadStoreComparator::DullahanReadStoreComparator(const std::vector<TableSchema_Column_ColumnType> & columns) :
    columns_{columns},
    bytewise_comparator_{rocksdb::BytewiseComparator()}
  {}

int DullahanReadStoreComparator::Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
  if (a.size() < kSizeOfColumn || b.size() < kSizeOfColumn) {
    return bytewise_comparator_->Compare(a, b);
  }

  // Ensure we're comparing the same column.
  int column_compare = memcmp(a.data(), b.data(), kSizeOfColumn);
  if (column_compare != 0) {
    return column_compare;
  }

  column_t column = *reinterpret_cast<const column_t *>(a.data());

  if (column >= 0 && column < columns_.size()) {
    switch (columns_[column]) {
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_SMALLINT:
        return CompareNumericType<int16_t>(bytewise_comparator_, a, b);
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_INTEGER:
        return CompareNumericType<int32_t>(bytewise_comparator_, a, b);
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_BIGINT:
        return CompareNumericType<int64_t>(bytewise_comparator_, a, b);
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_FLOAT:
        return CompareNumericType<float>(bytewise_comparator_, a, b);
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_DOUBLE:
        return CompareNumericType<double>(bytewise_comparator_, a, b);
      default:
        return bytewise_comparator_->Compare(a, b);
    }
  } else {
    return bytewise_comparator_->Compare(a, b);
  }
}


// TODO - Implement them

void DullahanReadStoreComparator::FindShortestSeparator(std::string *, const rocksdb::Slice &) const {

}

void DullahanReadStoreComparator::FindShortSuccessor(std::string *) const {

}

}