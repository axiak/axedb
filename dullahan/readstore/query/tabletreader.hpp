#pragma once
#ifndef __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_
#define __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_

#include "../tablet.hpp"

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <ewah.h>
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

  template<typename Function>
  void GetAllRecords(Function callable) const {
    rocksdb::Status status;
    Record currentRecord{};

    rocksdb::Iterator* it = db_->NewIterator(env_->getReadStoreReadOptions());
    for (it->Seek(ReadStoreKey::materializedRowKey(0).toSlice()); it->Valid() && ReadStoreKey::isMaterializedRowKey(it->key()); it->Next()) {
      currentRecord.ParseFromString(it->value().ToString());
      callable(currentRecord);
    }

  }

  uint32_t CountExactByColumn(column_t column, const std::string &value) const {
    rocksdb::Status status;
    ReadStoreKey key{column, value};
    std::string result;

    status = db_->Get(env_->getReadStoreReadOptions(), key.toSlice(), &result);
    if (status.IsNotFound()) {
      return 0;
    } else if (!status.ok()) {
      throw TabletLevelDbException{status};
    }

    EWAHBoolArray<bitarrayword> bitArray;
    bitArray.readStream(result.data(), result.size());

    return bitArray.numberOfOnes();
  }

  template<typename Function>
  void QueryExactByColumn(column_t column, const std::string &value, Function callable) const {
    rocksdb::Status status;
    ReadStoreKey key{column, value};
    std::string result;

    status = db_->Get(env_->getReadStoreReadOptions(), key.toSlice(), &result);
    if (status.IsNotFound()) {
      return;
    } else if (!status.ok()) {
      throw TabletLevelDbException{status};
    }

    EWAHBoolArray<bitarrayword> bitArray;
    bitArray.readStream(result.data(), result.size());

    result.clear();
    Record currentRecord;

    bitArray.iterateSetBits([&] (uint64_t pos) {
      rocksdb::Slice newKey = ReadStoreKey::materializedRowKey(pos).toSlice();
      status = db_->Get(
          env_->getReadStoreReadOptions(),
          newKey,
          &result
      );
      if (status.IsNotFound()) {
        return;
      }
      if (!status.ok()) {
        throw TabletLevelDbException{status};
      }

      currentRecord.ParseFromString(result);
      callable(currentRecord.id());
    });
  }
};

}


#endif // __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_