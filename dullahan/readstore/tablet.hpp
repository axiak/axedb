#pragma once
#ifndef _DULLAHAN_READSTORE_TABLET_H_
#define _DULLAHAN_READSTORE_TABLET_H_

#include <ios>

#include <rocksdb/db.h>
#include <ostream>
#include <iostream>
#include <type_traits>
#include <algorithm>
#include <unordered_map>

#include <ewah.h>

#include "../env.hpp"
#include "../protos/dullahan.pb.h"
#include "models/key.hpp"
#include "models/value.hpp"


namespace dullahan {

using namespace models;

class TabletWriter {
public:
  TabletWriter() =delete;
  TabletWriter(Env * env, long start_time, long stop_time);
  ~TabletWriter();

  // NonCopyable
  TabletWriter(const TabletWriter &) =delete;
  TabletWriter & operator=(const TabletWriter &) =delete;

  // Moveable, though.
  TabletWriter(TabletWriter && tablet) noexcept;
  TabletWriter & operator=(TabletWriter && tablet) noexcept;

  void compact();

  uint64_t watermark() const;

  template<typename _Iterable>
  void flushRecords(const _Iterable & iterable) {
    flushRecords(iterable.begin(), iterable.end());
  }

  // Add records to the tablet
  template<typename _IteratorType>
  void flushRecords(_IteratorType begin, _IteratorType end) {
    static_assert(
        std::is_same<
            typename std::iterator_traits<_IteratorType>::value_type,
            Record>::value,
        "Required Record::Reader iterator."
    );
    std::for_each(begin, end, [this](const Record & record) {
        addToScratch(record);
    });
    flushScratch();
  }

  static const std::shared_ptr<std::string> file_name(Env * env, long start_time, long stop_time) {
    std::shared_ptr<std::string> result(new std::string(env->tabletDir()));
    result->
            append("/")
        .append(std::to_string(start_time))
        .append("-")
        .append(std::to_string(stop_time));
    return result;
  }

private:
  Env * env;
  rocksdb::DB ** db;
  uint64_t id_watermark;
  uint64_t committed_id_watermark;
  uint64_t current_scratch_bit;
  uint64_t max_bitarray_bitsize;
  std::unordered_map<
      ReadStoreKey,
      ScratchValue,
      decltype(ReadStoreKey::hasher()),
      decltype(ReadStoreKey::equality())> scratch;

  void moveTablet(TabletWriter & dest, TabletWriter &&src);

  void addToScratch(const Record & record);
  void flushScratch();
  void commitWatermark();
  void readWatermark();


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