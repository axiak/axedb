#pragma once
#ifndef __DULLAHAN_READSTORE_TABLET_HPP_
#define __DULLAHAN_READSTORE_TABLET_HPP_

#include <boost/optional/optional.hpp>

#include "../protos/dullahan.pb.h"
#include "../env.hpp"
#include "rocksdb/columnordering.hpp"
#include <rocksdb/db.h>
#include <ios>

namespace dullahan {

using namespace models;

class Tablet {
private:
  std::shared_ptr<DullahanReadStoreComparator> comparator_;
protected:
  Env * env_;
  TabletMetadata tablet_metadata_;
  uint32_t written_highest_id_;
  rocksdb::DB * db_;

  Tablet() =delete;

  Tablet(Env * env, TabletMetadata tablet_metadata, const rocksdb::Options & dboptions);
  ~Tablet();

  Tablet(const Tablet &) =delete;
  Tablet & operator=(const Tablet &) =delete;

  Tablet(Tablet && tablet) noexcept;
  Tablet & operator=(Tablet && tablet) noexcept;

  uint32_t HighestIdAndIncrement(int increment_amount = 1);

  boost::optional<TabletMetadata> ReadMetadata();
  void UpdateIdWatermark(uint32_t highest_id);
  void WriteMetadata();

public:
  static const std::shared_ptr<std::string> FileName(Env * env, long start_time, long stop_time) {
    std::shared_ptr<std::string> result = std::make_shared<std::string>(env->tabletDir());
    result->
            append("/")
        .append(std::to_string(start_time))
        .append("-")
        .append(std::to_string(stop_time));
    return result;
  }

  static const std::shared_ptr<std::string> MetaDataFileName(Env * env, long start_time, long stop_time) {
    std::shared_ptr<std::string> result = FileName(env, start_time, stop_time);
    result->append(".metadata");
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

#endif // __DULLAHAN_READSTORE_TABLET_HPP_
