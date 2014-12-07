#include <iomanip>

#include "../dullahan.hpp"
#include "tablet.hpp"
#include "../protos/utils.hpp"

#include "models/key.hpp"

namespace dullahan {

using namespace models;

Tablet::Tablet(Env *env, long start_time, long stop_time):
    id_watermark{0},
    committed_id_watermark{0},
    num_bits_in_scratch{0},
    scratch{} {
  this->env = env;
  this->db = new rocksdb::DB*;
  rocksdb::Options options;
  env->updateLevelDbOptions(&options);
  rocksdb::Status status = rocksdb::DB::Open(options, *Tablet::file_name(env, start_time, stop_time), db);

  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }

  readWatermark();
}

Tablet::~Tablet() {
  // Check for null in case of a move
  if (db != nullptr) {
    delete db;
    db = nullptr;
  }
}

Tablet::Tablet(Tablet &&tablet) {
  moveTablet(*this, std::move(tablet));
}

Tablet &Tablet::operator=(Tablet &&tablet) {
  moveTablet(*this, std::move(tablet));
  return *this;
}

inline void Tablet::moveTablet(Tablet& dest, Tablet &&src) {
  if (dest.db != nullptr) {
    delete dest.db;
    dest.db = nullptr;
  }
  dest.env = std::move(src.env);
  dest.db = std::move(src.db);
}

void Tablet::addToScratch(const Record & record) {
  uint64_t current_id = id_watermark++;
  uint64_t current_bit = num_bits_in_scratch++;
  auto values = record.values();

  scratch[ReadStoreKey::materializedRowKey(current_id)].setRecord(record);

  std::for_each(values.begin(), values.end(), [this, current_bit](const Record_KeyValue & keyValue) {
    ReadStoreKey key(keyValue.column(), keyValue.value());
    ScratchValue & value = scratch[key];
    value.bitarray()->set(current_bit);
  });
}

void Tablet::flushScratch() {
  rocksdb::WriteOptions writeOptions;
  std::string buffer;

  uint64_t word_delta = EWAHBoolArray<word>::padToWord(id_watermark);
  id_watermark += word_delta;
  num_bits_in_scratch += word_delta;

  for (auto pair : scratch) {
    const rocksdb::Slice key = pair.first.toSlice();

    switch (pair.first.getKeyType()) {
      case ReadStoreKey::KeyType::COLUMN: {
        EWAHBoolArray<word> *newValue = pair.second.bitarray();
        newValue->padWithZeroes(num_bits_in_scratch);
        newValue->writeAsStream(&buffer);

        rocksdb::Slice newSlice{buffer};

        rocksdb::Status status;
        status = (*db)->Merge(writeOptions, key, newSlice);

        if (!status.ok()) {
          throw TabletLevelDbException(status);
        }
      };
        break;

      case ReadStoreKey::KeyType::MATERIALIZED_ROW: {
        rocksdb::Status status = (*db)->Put(writeOptions, key, models::toSlice(pair.second.record(), &buffer));
        if (!status.ok()) {
          throw TabletLevelDbException(status);
        }
      }
        break;
      case ReadStoreKey::KeyType::META:
        break;

    }
  }
  num_bits_in_scratch = 0;
  scratch.clear();
  commitWatermark();
}

void Tablet::commitWatermark() {
  uint64_t committing_watermark = id_watermark;
  rocksdb::WriteOptions writeOptions;

  rocksdb::Slice value{reinterpret_cast<const char *> (&committing_watermark), sizeof(id_watermark)};

  rocksdb::Status status = (*db)->Put(
      writeOptions,
      ReadStoreKey::metadataKey().toSlice(),
      value
  );
  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }

  committed_id_watermark = committing_watermark;
}

void Tablet::readWatermark() {
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

void Tablet::compact() {
  rocksdb::Status status = (*db)->CompactRange(nullptr, nullptr);
  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }
}


} // namespace dullahan