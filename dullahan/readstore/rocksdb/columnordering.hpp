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
  DullahanReadStoreComparator(const TableSchema &table_schema);

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const;

  void FindShortestSeparator(std::string *, const rocksdb::Slice &) const;

  void FindShortSuccessor(std::string *) const;

  const char *Name() const {
    return "DullahanReadStoreComparator";
  }

  static std::shared_ptr<DullahanReadStoreComparator> createComparator(const TableSchema &table_schema) {
    return std::make_shared<DullahanReadStoreComparator>(table_schema);
  }

private:
  std::vector<TableSchema_Column> columns_;
  const rocksdb::Comparator * bytewise_comparator_;
};

}

#endif // __DULLAHAN_READSTORE_ROCKSDB_COLUMN_ORDERING_HPP_