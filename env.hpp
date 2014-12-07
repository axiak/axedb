#pragma once
#ifndef _DULLAHAN_ENV_H_
#define _DULLAHAN_ENV_H_

#include <string>
#include <utility>
#include <rocksdb/options.h>

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
  }

  void updateLevelDbOptions(rocksdb::Options * options) const;

  static Env * getEnv() {
    static Env env;
    return &env;
  };


private:
  Env();

  std::string data_dir_;
};


}


#endif // _DULLAHAN_ENV_H_