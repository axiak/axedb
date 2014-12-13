#pragma once
#ifndef __DULLAHAN_READSTORE_ROCKSDB_COLUMN_ORDERING_HPP_
#define __DULLAHAN_READSTORE_ROCKSDB_COLUMN_ORDERING_HPP_

#include <rocksdb/comparator.h>
#include <memory>
#include <vector>

#include "../../protos/dullahan.pb.h";

namespace dullahan {

using namespace models;

class DullahanReadStoreComparator : public rocksdb::Comparator {
public:
  DullahanReadStoreComparator(const std::vector<TableSchema_Column_ColumnType> & columns);

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const;

  void FindShortestSeparator(std::string *, const rocksdb::Slice &) const;

  void FindShortSuccessor(std::string *) const;

  const char *Name() const {
    return "DullahanReadStoreComparator";
  }

  static std::vector<TableSchema_Column_ColumnType> BuildColumnVector(const TableSchema &table_schema) {
    std::vector<TableSchema_Column_ColumnType> result{};
    result.reserve(table_schema.columns_size());
    for (auto column : table_schema.columns()) {
      result.emplace_back(column.type());
    }
    return result;
  }

  static std::shared_ptr<DullahanReadStoreComparator> CreateComparator(const std::vector<TableSchema_Column_ColumnType> columns) {
    return std::make_shared<DullahanReadStoreComparator>(columns);
  }

private:
  std::vector<TableSchema_Column_ColumnType> columns_;
  const rocksdb::Comparator * bytewise_comparator_;
};

}

#endif // __DULLAHAN_READSTORE_ROCKSDB_COLUMN_ORDERING_HPP_