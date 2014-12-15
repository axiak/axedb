
#include "value.hpp"

namespace dullahan {
namespace models {


ScratchValue::ScratchValue() :
    record_{nullptr},
    bitarray_{} {}

ScratchValue::ScratchValue(const ScratchValue & s) :
    record_{s.record_},
    bitarray_{s.bitarray_} {}

ScratchValue::ScratchValue(ScratchValue&& s) :
    record_{std::move(s.record_)},
    bitarray_{std::move(s.bitarray_)} {}


void ScratchValue::setRecord(const Record & record) {
  record_ = &record;
}

ScratchValue & ScratchValue::operator=(const ScratchValue & s) {
  record_ = s.record_;
  bitarray_ = s.bitarray_;
  return *this;
}
ScratchValue & ScratchValue::operator=(ScratchValue && s) {
  record_ = std::move(s.record_);
  bitarray_ = std::move(s.bitarray_);
  return *this;
}

BitArray * ScratchValue::bitarray() {
  return &bitarray_;
}

const BitArray * const ScratchValue::bitarray() const {
  return &bitarray_;
}

const Record & ScratchValue::record() const {
  return *record_;
}

}
}
