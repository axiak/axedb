#pragma once
#ifndef _DULLAHAN_ENV_H_
#define _DULLAHAN_ENV_H_

#include <string>
#include <utility>
#include <rocksdb/options.h>
#include <sys/stat.h>

namespace dullahan {

namespace constants {
const std::string TABLET_DIRECTORY = "tablets";
}

class Env {

public:
  Env(const Env & env) =delete;
  void operator=(const Env & env) =delete;

  const std::string dataDir() const;
  const std::string tabletDir() const;

  template <typename T>
  void setDataDir(T&& data_dir) {
    data_dir_ = std::forward<T>(data_dir);
    std::string tablet_dir{data_dir};
    tablet_dir.append("/").append(constants::TABLET_DIRECTORY);
    mkdir(tablet_dir.data(), 0700);
    updateRocksDbOptions();
  }

  const rocksdb::Options &getReadStoreWritingOptions() const;
  const rocksdb::Options &getReadStoreReadingOptions() const;
  const rocksdb::WriteOptions & getReadStoreWriteOptions() const;
  const rocksdb::ReadOptions & getReadStoreReadOptions() const;

  static Env * getEnv() {
    static Env env;
    return &env;
  };


private:
  Env();
  void updateRocksDbOptions();
  std::string data_dir_;
  rocksdb::Options rocksdb_write_readstore_options;
  rocksdb::Options rocksdb_readstore_options;
  rocksdb::WriteOptions rocksdb_write_options;
  rocksdb::ReadOptions rocksdb_read_options;
};


}


#endif // _DULLAHAN_ENV_H_