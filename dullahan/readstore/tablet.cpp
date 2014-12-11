#include <iomanip>

#include "../dullahan.hpp"
#include "tablet.hpp"
#include "../protos/utils.hpp"

namespace dullahan {

using namespace models;

constexpr size_t bitsInWord = sizeof(bitarrayword) * 8;

TabletWriter::TabletWriter(Env *env, long start_time, long stop_time):
    id_watermark{0},
    committed_id_watermark{0},
    current_scratch_bit{0},
    max_bitarray_bitsize{0},
    scratch{} {
  this->env = env;
  this->db = new rocksdb::DB*;

  rocksdb::Status status = rocksdb::DB::Open(
      env->getReadStoreWritingOptions(),
      *TabletWriter::file_name(env, start_time, stop_time),
      db
  );

  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }

  readWatermark();
}

TabletWriter::~TabletWriter() {
  // Check for null in case of a move
  if (*db != nullptr) {
    delete *db;
    db = nullptr;
  }
}

TabletWriter::TabletWriter(TabletWriter &&tablet) noexcept {
  moveTablet(*this, std::move(tablet));
}

TabletWriter &TabletWriter::operator=(TabletWriter &&tablet) noexcept {
  moveTablet(*this, std::move(tablet));
  return *this;
}

inline void TabletWriter::moveTablet(TabletWriter & dest, TabletWriter &&src) {
  if (dest.db != nullptr) {
    delete dest.db;
    dest.db = nullptr;
  }
  dest.env = std::move(src.env);
  dest.db = std::move(src.db);
}

void TabletWriter::addToScratch(const Record & record) {
  uint64_t current_id = id_watermark++;
  uint64_t current_bit = current_scratch_bit++;
  auto values = record.values();

  scratch[ReadStoreKey::materializedRowKey(current_id)].setRecord(record);

  std::for_each(values.begin(), values.end(), [this, current_bit, &record, current_id](const Record_KeyValue & keyValue) {
    ReadStoreKey key(keyValue.column(), keyValue.value());
    ScratchValue & value = scratch[key];
    value.bitarray()->set(current_bit);
    const uint64_t bitsize = value.bitarray()->bufferSize() * bitsInWord;
    if (bitsize > max_bitarray_bitsize) {
      max_bitarray_bitsize = bitsize;
    }
  });
}

void TabletWriter::flushScratch() {
  std::string buffer;

  uint64_t word_delta = EWAHBoolArray<word>::padToWord(max_bitarray_bitsize);
  max_bitarray_bitsize += word_delta;
  id_watermark += max_bitarray_bitsize - current_scratch_bit;
  current_scratch_bit += word_delta;


  for (auto pair : scratch) {
    const rocksdb::Slice key = pair.first.toSlice();

    switch (pair.first.getKeyType()) {
      case ReadStoreKey::KeyType::COLUMN: {
        buffer.resize(0);
        EWAHBoolArray<bitarrayword> *newValue = pair.second.bitarray();
        newValue->padWithZeroes(current_scratch_bit);
        newValue->appendToString(&buffer, committed_id_watermark);

        rocksdb::Slice newSlice{buffer};

        rocksdb::Status status;
        status = (*db)->Merge(env->getReadStoreWriteOptions(), key, newSlice);

        if (!status.ok()) {
          throw TabletLevelDbException(status);
        }
      };
        break;

      case ReadStoreKey::KeyType::MATERIALIZED_ROW: {
        rocksdb::Status status = (*db)->Put(
            env->getReadStoreWriteOptions(),
            key,
            models::toSlice(pair.second.record(), &buffer)
        );

        if (!status.ok()) {
          throw TabletLevelDbException(status);
        }
      }
        break;
      case ReadStoreKey::KeyType::META:
        break;

    }
  }
  max_bitarray_bitsize = 0;
  current_scratch_bit = 0;
  scratch.clear();
  commitWatermark();
}

void TabletWriter::commitWatermark() {
  uint64_t committing_watermark = id_watermark;
  rocksdb::Slice value{reinterpret_cast<const char *> (&committing_watermark), sizeof(id_watermark)};

  rocksdb::Status status = (*db)->Put(
      env->getReadStoreWriteOptions(),
      ReadStoreKey::metadataKey().toSlice(),
      value
  );
  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }

  committed_id_watermark = committing_watermark;
}

void TabletWriter::readWatermark() {
  rocksdb::ReadOptions readOptions;
  std::string value;
  rocksdb::Status status = (*db)->Get(readOptions, ReadStoreKey::metadataKey().toSlice(), &value);
  if (!status.ok() && !status.IsNotFound()) {
    throw TabletLevelDbException(status);
  }
  if (status.ok()) {
    committed_id_watermark = id_watermark = *reinterpret_cast<const unsigned long *>(value.data());
  }
}

void TabletWriter::compact() {
  rocksdb::Status status = (*db)->CompactRange(nullptr, nullptr);
  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }
}

uint64_t TabletWriter::watermark() const {
  return id_watermark;
}

} // namespace dullahan