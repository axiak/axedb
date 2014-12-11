#include <rocksdb/statistics.h>
#include <util/auto_roll_logger.h>
#include <rocksdb/memtablerep.h>
#include "env.hpp"
#include "readstore/ewahmerge.hpp"

#include "utils/bytes.hpp"

namespace dullahan {

Env::Env() :
    data_dir_{},
    rocksdb_write_readstore_options{},
    rocksdb_readstore_options{},
    rocksdb_write_options{},
    rocksdb_read_options{}
   {
     updateRocksDbOptions();
   }

const std::string Env::dataDir() const {
  return data_dir_;
}

const std::string Env::tabletDir() const {
  return data_dir_ + "/" + constants::TABLET_DIRECTORY;
}

const rocksdb::Options & Env::getReadStoreWritingOptions() const {
  return rocksdb_write_readstore_options;
}

const rocksdb::Options & Env::getReadStoreReadingOptions() const {
  return rocksdb_readstore_options;
}

const rocksdb::WriteOptions & Env::getReadStoreWriteOptions() const {
  return rocksdb_write_options;
}

const rocksdb::ReadOptions & Env::getReadStoreReadOptions() const {
  return rocksdb_read_options;
}

void Env::updateRocksDbOptions() {
 using namespace dullahan::bytes;

  rocksdb_write_readstore_options.disableDataSync = true;
  rocksdb_write_readstore_options.create_if_missing = true;
  rocksdb_write_readstore_options.merge_operator = EWAHMergeOperator::createEWAHMergeOperator();
  rocksdb_write_readstore_options.target_file_size_base = 1_GB;
  rocksdb_write_options.disableWAL = true;
  rocksdb_write_options.sync = false;
  rocksdb_write_readstore_options.use_fsync = false;
  rocksdb_write_readstore_options.statistics = rocksdb::CreateDBStatistics();
  rocksdb_write_readstore_options.stats_dump_period_sec = 10;
  rocksdb_write_readstore_options.disable_auto_compactions = true;
  std::shared_ptr<rocksdb::Logger> logger;
  rocksdb::DBOptions dbOptions;
  rocksdb_write_readstore_options.num_levels = 2;
  rocksdb_write_readstore_options.source_compaction_factor = 10000000;
  rocksdb_write_readstore_options.table_cache_numshardbits = 4;
  rocksdb_write_readstore_options.write_buffer_size = 256_MB;
  rocksdb_write_readstore_options.max_write_buffer_number = 30;
  rocksdb_write_readstore_options.max_background_compactions = 20;

  rocksdb_write_readstore_options.level0_file_num_compaction_trigger = 10000000;
  rocksdb_write_readstore_options.level0_slowdown_writes_trigger = 10000000;
  rocksdb_write_readstore_options.level0_stop_writes_trigger = 10000000;
  rocksdb_write_readstore_options.max_bytes_for_level_base = 1_MB;
  rocksdb_write_readstore_options.memtable_factory.reset(new rocksdb::VectorRepFactory);
  rocksdb_write_readstore_options.max_grandparent_overlap_factor = 10;
  rocksdb_write_readstore_options.error_if_exists = true;

  rocksdb_readstore_options.table_cache_numshardbits = 4;
  rocksdb_readstore_options.allow_mmap_reads = true;

  dbOptions.stats_dump_period_sec = 10;

  rocksdb::CreateLoggerFromOptions(
    "read-only-tablet",
      "/tmp",
      ::rocksdb::Env::Default(),
      dbOptions,
      &logger
  );

  rocksdb_write_readstore_options.info_log = logger;
  //rocksdb_options.error_if_exists = true;
}



}