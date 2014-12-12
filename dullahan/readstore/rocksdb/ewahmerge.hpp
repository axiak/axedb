#pragma once
#ifndef __DULLAHAN_EWAHMERGE_H_
#define __DULLAHAN_EWAHMERGE_H_

#include <rocksdb/merge_operator.h>

namespace dullahan {

class EWAHMergeOperator : public rocksdb::AssociativeMergeOperator {

public:
  virtual bool Merge(
      const rocksdb::Slice& key,
      const rocksdb::Slice* existing_value,
      const rocksdb::Slice& value,
      std::string* new_value,
      rocksdb::Logger* logger) const override;

  virtual const char* Name() const override {
    return "EWAHMergeOperator";
  }

  static std::shared_ptr<EWAHMergeOperator> createEWAHMergeOperator() {
    return std::make_shared<EWAHMergeOperator>();
  }
};


}

#endif // __DULLAHAN_EWAHMERGE_H_

