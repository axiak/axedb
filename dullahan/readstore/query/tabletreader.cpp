#include "tabletreader.hpp"
#include "../tablet.hpp"

namespace dullahan {

using namespace models;


TabletReader::TabletReader(Env *env, long start_time, long stop_time) {
  this->env = env;
  this->db = new rocksdb::DB*;

  rocksdb::Status status = rocksdb::DB::Open(
      env->getReadStoreReadingOptions(),
      *TabletWriter::file_name(env, start_time, stop_time),
      db
  );

  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }
}

TabletReader::~TabletReader() {
  // Check for null in case of a move
  if (db != nullptr) {
    delete db;
    db = nullptr;
  }
}

TabletReader::TabletReader(TabletReader &&tablet) noexcept {
  moveTablet(*this, std::move(tablet));
}

TabletReader &TabletReader::operator=(TabletReader &&tablet) noexcept {
  moveTablet(*this, std::move(tablet));
  return *this;
}


inline void TabletReader::moveTablet(TabletReader & dest, TabletReader &&src) {
  if (dest.db != nullptr) {
    delete dest.db;
    dest.db = nullptr;
  }
  dest.env = std::move(src.env);
  dest.db = std::move(src.db);
}

}