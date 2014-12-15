#pragma once
#ifndef __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_
#define __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_

#include "../tablet.hpp"
#include <functional>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <ewah.h>
#include <boost/optional.hpp>
#include "../../env.hpp"
#include "../models/key.hpp"

#include "../../protos/dullahan.pb.h"
#include "../models/base.hpp"

namespace dullahan {

using namespace models;

class TabletReader: public Tablet {
public:
  TabletReader() = delete;

  TabletReader(Env *env, const TabletMetadata & tabletMetadata);

  TabletReader(const TabletReader &) = delete;
  TabletReader &operator=(const TabletReader &) = delete;

  TabletReader(TabletReader &&tablet) noexcept;
  TabletReader &operator=(TabletReader &&tablet) noexcept;

  void GetAllRecords(std::function<void(const Record &)> callable) const;

  uint32_t CountExactByColumn(column_t column, const std::string &value) const;

  uint32_t TotalRowCount() const;

  BitArray GetBitArray(const ReadStoreKey & key) const;

  void GetBitArrays(column_t column, const ReadStoreKey & start_key, const ReadStoreKey & stop_key, std::function<void(BitArray)> callable) const;

  void QueryExactByColumn(column_t column, const std::string &value, std::function<void(const Record &)> callable) const;

  BitArray WhereClause(const Query_Predicate & predicate) const;

  uint32_t CountWhere(const Query_Predicate & predicate) const;

  const TableSchema & table_schema() const;
};

}


#endif // __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_