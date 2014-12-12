#include <rocksdb/db.h>
#include <fstream>
#include <boost/optional/optional.hpp>
#include <stdint-gcc.h>

#include "tablet.hpp"
#include "../protos/utils.hpp"
#include "./models/base.hpp"


namespace dullahan {

void SetSystemData(TabletMetadata & tablet_metadata);

Tablet::Tablet(Env * env, TabletMetadata tablet_metadata, const rocksdb::Options & dboptions) :
    env_{env},
    tablet_metadata_{tablet_metadata},
    written_highest_id_{0} {
  rocksdb::Status status = rocksdb::DB::Open(
      dboptions,
      *FileName(env, tablet_metadata.timestamp_start(), tablet_metadata.timestamp_stop()),
      &db_
  );

  if (!status.ok()) {
    throw TabletLevelDbException(status);
  }
}

Tablet::~Tablet() {
  if (db_ != nullptr) {
    delete db_;
    db_ = nullptr;
  }
}

// Move operations
Tablet::Tablet(Tablet &&tablet) noexcept :
    db_{std::move(tablet.db_)},
    env_{std::move(tablet.env_)},
    tablet_metadata_{std::move(tablet.tablet_metadata_)},
    written_highest_id_{tablet.written_highest_id_}
{}

Tablet &Tablet::operator=(Tablet &&tablet) noexcept {
  db_ = std::move(tablet.db_);
  env_ = std::move(tablet.env_);
  tablet_metadata_ = std::move(tablet.tablet_metadata_);
  written_highest_id_ = tablet.written_highest_id_;
}

void Tablet::UpdateIdWatermark(uint32_t highest_id) {
  tablet_metadata_.set_highest_id(highest_id);
}


uint32_t Tablet::HighestIdAndIncrement(int increment_amount) {
  uint32_t highest_id = tablet_metadata_.highest_id();
  tablet_metadata_.set_highest_id(highest_id + increment_amount);
  return highest_id;
}

void Tablet::WriteMetadata() {
  std::ofstream ofstream;

  SetSystemData(tablet_metadata_);

  ofstream.open(*MetaDataFileName(env_, tablet_metadata_.timestamp_start(), tablet_metadata_.timestamp_stop()), std::ios::out | std::ios::trunc);
  tablet_metadata_.SerializeToOstream(&ofstream);
  written_highest_id_ = tablet_metadata_.highest_id();
}

boost::optional<TabletMetadata> Tablet::ReadMetadata() {
  boost::optional<TabletMetadata> result{};

  std::ifstream ifstream;
  ifstream.open(*MetaDataFileName(env_, tablet_metadata_.timestamp_start(), tablet_metadata_.timestamp_stop()), std::ios::in);

  result->ParseFromIstream(&ifstream);

  return result;
}


void SetSystemData(TabletMetadata & tablet_metadata) {
  tablet_metadata.set_tablet_version(TabletMetadata_TabletVersion::TabletMetadata_TabletVersion_ONE);
  tablet_metadata.set_endianness(IsBigEndian() ?
      TabletMetadata_Endianness::TabletMetadata_Endianness_BIG :
      TabletMetadata_Endianness::TabletMetadata_Endianness_LITTLE);
  tablet_metadata.set_size_of_bitword(kSizeOfBitArrayBytes);
}

// TODO - implement
void CheckSystemDataOrDie(const TabletMetadata & tablet_metadata) {

}

}