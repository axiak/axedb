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

#include <rocksdb/db.h>
#include <ewah.h>


#include "../env.hpp"
#include "../protos/dullahan.pb.h"
#include "models/key.hpp"
#include "models/value.hpp"
#include "tablet.hpp"


namespace dullahan {

using namespace models;
class TabletWriter: public Tablet {
public:
  TabletWriter() =delete;
  TabletWriter(Env * env, const TabletMetadata & tablet_metadata);

  TabletWriter(const TabletWriter &) =delete;
  TabletWriter & operator=(const TabletWriter &) =delete;

  TabletWriter(TabletWriter && tablet) noexcept;
  TabletWriter & operator=(TabletWriter && tablet) noexcept;

  void Compact();

  template<typename _Iterable>
  void FlushRecords(const _Iterable & iterable) {
    FlushRecords(iterable.begin(), iterable.end());
  }

  // Add records to the tablet
  template<typename _IteratorType>
  void FlushRecords(_IteratorType begin, _IteratorType end) {
    static_assert(
        std::is_same<
            typename std::iterator_traits<_IteratorType>::value_type,
            Record>::value,
        "Required Record::Reader iterator."
    );
    std::for_each(begin, end, [this](const Record & record) {
        AddToScratch(record);
    });
    FlushScratch();
  }



private:
  uint32_t current_scratch_bit_;
  uint32_t max_bitarray_bitsize_;
  std::unordered_map<
      ReadStoreKey,
      ScratchValue,
      decltype(ReadStoreKey::hasher()),
      decltype(ReadStoreKey::equality())> scratch_;

  void AddToScratch(const Record & record);
  void AddToBitArray(const uint32_t record_id, const ReadStoreKey & key);
  void FlushScratch();
};




}

#endif // _DULLAHAN_READSTORE_TABLET_H_