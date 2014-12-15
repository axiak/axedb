#include <iomanip>

#include "../dullahan.hpp"
#include "tabletwriter.hpp"
#include "../protos/utils.hpp"
#include "./rocksdb/columnordering.hpp"

namespace dullahan {

using namespace models;


TabletWriter::TabletWriter(Env *env, const TabletMetadata & tablet_metadata):
    Tablet(env, tablet_metadata, env->getReadStoreWritingOptions()),
    current_scratch_bit_{0},
    max_bitarray_bitsize_{0},
    scratch_{} {
}

TabletWriter::TabletWriter(TabletWriter &&tablet) noexcept :
  Tablet(std::move(tablet)),
  scratch_{} {
}

TabletWriter &TabletWriter::operator=(TabletWriter &&tablet) noexcept {
  Tablet::operator=(std::move(tablet));
  current_scratch_bit_ = tablet.current_scratch_bit_;
  max_bitarray_bitsize_ = tablet.max_bitarray_bitsize_;
  return *this;
}

void TabletWriter::AddToBitArray(const uint32_t record_id, const ReadStoreKey & key) {
  ScratchValue & value = scratch_[key];
  value.bitarray()->set(record_id);
  const uint32_t bitsize = static_cast<uint32_t>(value.bitarray()->bufferSize() * kSizeOfBitArrayBits);
  if (bitsize > max_bitarray_bitsize_) {
    max_bitarray_bitsize_ = bitsize;
  }
}


void TabletWriter::AddToScratch(const Record & record) {
  const uint32_t current_id = HighestIdAndIncrement();
  const uint32_t current_bit = current_scratch_bit_++;
  auto values = record.values();

  scratch_[ReadStoreKey::MaterializedRowKey(current_id)].setRecord(record);
  AddToBitArray(current_bit, ReadStoreKey::AllRowsKey());

  std::for_each(values.begin(), values.end(), [this, current_bit](const Record_KeyValue & keyValue) {
    ReadStoreKey key(keyValue.column(), keyValue.value());
    AddToBitArray(current_bit, key);
    AddToBitArray(current_bit, ReadStoreKey::AllRowsForColumnKey(keyValue.column()));
  });
}


void TabletWriter::FlushScratch() {
  std::string buffer;

  uint64_t word_delta = EWAHBoolArray<word>::padToWord(max_bitarray_bitsize_);
  max_bitarray_bitsize_ += word_delta;
  HighestIdAndIncrement(max_bitarray_bitsize_ - current_scratch_bit_);
  current_scratch_bit_ += word_delta;


  for (auto pair : scratch_) {
    const rocksdb::Slice key = pair.first.ToSlice();

    switch (pair.first.getKeyType()) {
      case ReadStoreKey::KeyType::COLUMN: {
        buffer.resize(0);
        BitArray *newValue = pair.second.bitarray();
        newValue->padWithZeroes(current_scratch_bit_);
        newValue->appendToString(&buffer, written_highest_id_);

        rocksdb::Slice newSlice{buffer};

        rocksdb::Status status;
        status = db_->Merge(env_->getReadStoreWriteOptions(), key, newSlice);

        if (!status.ok()) {
          throw TabletLevelDbException(status);
        }
      };
        break;

      case ReadStoreKey::KeyType::MATERIALIZED_ROW: {
        rocksdb::Status status = db_->Put(
            env_->getReadStoreWriteOptions(),
            key,
            models::ToSlice(pair.second.record(), &buffer)
        );

        if (!status.ok()) {
          throw TabletLevelDbException(status);
        }
      }
        break;

    }
  }
  max_bitarray_bitsize_ = 0;
  current_scratch_bit_ = 0;
  scratch_.clear();
  WriteMetadata();
}

void TabletWriter::Compact() {
  rocksdb::Status status = db_->CompactRange(nullptr, nullptr);
  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }
}

} // namespace dullahan