#pragma once
#ifndef __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_
#define __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_

#include "../../env.hpp"
#include "../models/key.hpp"
#include "../tablet.hpp"

namespace dullahan {

class TabletReader {
public:
  TabletReader() = delete;

  TabletReader(Env *env, long start_time, long stop_time);

  ~TabletReader();

  // NonCopyable
  TabletReader(const TabletReader &) = delete;
  TabletReader &operator=(const TabletReader &) = delete;

  // Moveable, though.
  TabletReader(TabletReader &&tablet) noexcept;
  TabletReader &operator=(TabletReader &&tablet) noexcept;

  template<typename Function>
  void getAllRecords(Function callable) const {
    rocksdb::Status status;
    Record currentRecord{};

    rocksdb::Iterator* it = (*db)->NewIterator(env->getReadStoreReadOptions());
    for (it->Seek(ReadStoreKey::materializedRowKey(0).toSlice()); it->Valid() && ReadStoreKey::isMaterializedRowKey(it->key()); it->Next()) {
      currentRecord.ParseFromString(it->value().ToString());
      callable(currentRecord);
    }

  }

  uint32_t countExactByColumn(column_t column, const std::string &value) const {
    rocksdb::Status status;
    ReadStoreKey key{column, value};
    std::string result;

    status = (*db)->Get(env->getReadStoreReadOptions(), key.toSlice(), &result);
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
  void queryExactByColumn(column_t column, const std::string &value, Function callable) const {
    rocksdb::Status status;
    ReadStoreKey key{column, value};
    std::string result;

    status = (*db)->Get(env->getReadStoreReadOptions(), key.toSlice(), &result);
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
      status = (*db)->Get(
          env->getReadStoreReadOptions(),
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

private:
  inline void moveTablet(TabletReader & dest, TabletReader &&src);
  Env * env;
  rocksdb::DB ** db;
};

}


#endif // __DULLAHAN_READSTORE_QUERY_TABLETREADER_HPP_