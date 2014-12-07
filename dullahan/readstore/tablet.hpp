#pragma once
#ifndef _DULLAHAN_READSTORE_TABLET_H_
#define _DULLAHAN_READSTORE_TABLET_H_

#include <ios>

#include <rocksdb/db.h>
#include <ostream>
#include <iostream>
#include <type_traits>
#include <algorithm>
#include <google/sparse_hash_map>

#include <ewah.h>

#include "../env.hpp"
#include "../records/record.pb.h"
#include "key.hpp"


namespace dullahan {

class ScratchValue {
public:
  ScratchValue();
  ScratchValue(const Record & record);
  ~ScratchValue() =default;
  ScratchValue(const ScratchValue &);
  ScratchValue(ScratchValue&&);

  ScratchValue & operator=(const ScratchValue &);
  ScratchValue & operator=(ScratchValue &&);

  void setRecord(const Record & record);

  EWAHBoolArray<uint32_t> * bitarray();
  const EWAHBoolArray<uint32_t> * const bitarray() const;
  const Record * const record() const;
  const rocksdb::Slice recordValue(std::string * buffer) const;

private:
  EWAHBoolArray<uint32_t> bitarray_;
  const Record * record_;
};

class Tablet {
public:
  Tablet() =delete;
  Tablet(Env * env, long start_time, long stop_time);
  ~Tablet();

  // NonCopyable
  Tablet(const Tablet &) =delete;
  Tablet & operator=(const Tablet &) =delete;

  // Moveable, though.
  Tablet(Tablet&& tablet);
  Tablet & operator=(Tablet && tablet);

  void compact();

  // Add records to the tablet
  template<typename _IteratorType>
  void flushRecords(_IteratorType begin, _IteratorType end) {
    static_assert(
        std::is_same<
            typename std::iterator_traits<_IteratorType>::value_type,
            Record>::value,
        "Required Record::Reader iterator."
    );
    std::for_each(begin, end, [this](Record & record) {
        addToScratch(record);
    });
    std::cout << "Size of scratch: " << scratch.size() << std::endl;
    flushScratch();
  }

private:
  Env * env;
  rocksdb::DB ** db;
  uint64_t volatile id_watermark;
  uint64_t committed_id_watermark;
  uint64_t num_bits_in_scratch;
  google::sparse_hash_map<ReadStoreKey, ScratchValue, decltype(ReadStoreKey::hasher()), decltype(ReadStoreKey::equality())> scratch;
      /*
      sparse_hash_map<const char*, int, hash<const char*>, eqstr> months;
       */

  void moveTablet(Tablet& dest, Tablet &&src);

  void addToScratch(Record & record);
  void flushScratch();
  void commitWatermark();
  void readWatermark();

  static const std::shared_ptr<std::string> file_name(Env * env, long start_time, long stop_time) {
    std::shared_ptr<std::string> result(new std::string(env->tabletDir()));
    result->
         append("/")
        .append(std::to_string(start_time))
        .append("-")
        .append(std::to_string(stop_time));
    return result;
  }
};

class TabletLevelDbException : std::ios_base::failure {
public:
  TabletLevelDbException(rocksdb::Status status) :
      failure(status.ToString()),
      status_(status)
  {}

  virtual const char*
      what() const throw() {
    return status_.ToString().c_str();
  }

private:
  rocksdb::Status status_;


};


}

#endif // _DULLAHAN_READSTORE_TABLET_H_