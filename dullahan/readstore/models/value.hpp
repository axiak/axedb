#pragma once
#ifndef __DULLAHAN_READSTORE_MODELS_VALUE_HPP_
#define __DULLAHAN_READSTORE_MODELS_VALUE_HPP_

#include <utility>
#include <ewah.h>

#include "../../protos/dullahan.pb.h"

namespace dullahan {
namespace models {


class ScratchValue {
public:
  ScratchValue();
  ~ScratchValue() =default;
  ScratchValue(const ScratchValue &);
  ScratchValue(ScratchValue&&);

  ScratchValue & operator=(const ScratchValue &);
  ScratchValue & operator=(ScratchValue &&);

  void setRecord(const Record & record);

  EWAHBoolArray<uint32_t> * bitarray();
  const EWAHBoolArray<uint32_t> * const bitarray() const;
  const Record & record() const;

private:
  const Record * record_;
  EWAHBoolArray<uint32_t> bitarray_;
};


}
}

#endif // __DULLAHAN_READSTORE_MODELS_VALUE_HPP_