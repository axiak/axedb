#include "env.hpp"
#include "readstore/ewahmerge.hpp"


namespace dullahan {

Env::Env() :
    data_dir_()
   {}

const std::string Env::dataDir() const {
  return data_dir_;
}

void Env::updateLevelDbOptions(rocksdb::Options *options) const {
  options->create_if_missing = true;
  options->merge_operator = EWAHMergeOperator::createEWAHMergeOperator();
}

const std::string Env::tabletDir() const {
  return data_dir_ + "/" + constants::TABLET_DIRECTORY;
}


}