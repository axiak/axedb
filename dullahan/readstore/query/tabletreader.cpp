#include "tabletreader.hpp"
#include "./predicate_operators.cpp"

namespace dullahan {

using namespace models;

TabletReader::TabletReader(Env *env, const TabletMetadata &tablet_metadata) :
    Tablet(env, tablet_metadata, env->getReadStoreReadingOptions()) {
  boost::optional<TabletMetadata> maybeMetadata = ReadMetadata();
  if (maybeMetadata) {
    tablet_metadata_ = *maybeMetadata;
  }
}

TabletReader::TabletReader(TabletReader &&tablet) noexcept :
    Tablet(std::move(tablet)) {
}

TabletReader &TabletReader::operator=(TabletReader &&tablet) noexcept {
  Tablet::operator=(std::move(tablet));
  return *this;
}

void TabletReader::GetAllRecords(std::function<void(const Record &)> callable) const {
  rocksdb::Status status;
  Record currentRecord{};

  rocksdb::Iterator* it = db_->NewIterator(env_->getReadStoreReadOptions());
  for (it->Seek(ReadStoreKey::MaterializedRowKey(0).ToSlice()); it->Valid() && ReadStoreKey::IsMaterializedRowKey(it->key()); it->Next()) {
    currentRecord.ParseFromString(it->value().ToString());
    callable(currentRecord);
  }
}

BitArray TabletReader::GetBitArray(const ReadStoreKey & key) const {
  rocksdb::Status status;
  std::string result;
  BitArray bit_array;

  status = db_->Get(env_->getReadStoreReadOptions(), key.ToSlice(), &result);
  if (status.IsNotFound()) {
    return bit_array;
  } else if (!status.ok()) {
    throw TabletLevelDbException{status};
  }

  bit_array.readStream(result.data(), result.size());
  return bit_array;
}

void TabletReader::GetBitArrays(column_t column, const ReadStoreKey & start_key, const ReadStoreKey & stop_key, std::function<void(BitArray)> callable) const {
  rocksdb::Status status;
  std::string result;

  rocksdb::Iterator* it = db_->NewIterator(env_->getReadStoreReadOptions());
  rocksdb::Slice stop_key_slice = stop_key.ToSlice();

  BitArray bit_array;

  for (it->Seek(start_key.ToSlice()); it->Valid() && it->key().compare(stop_key_slice) < 0; it->Next()) {
    if (it->key().size() >= sizeof(column_t) &&
        strncmp(it->key().data(), reinterpret_cast<const char *>(&column), sizeof(column_t)) == 0) {
      bit_array.readStream(it->value().data(), it->value().size());
      callable(bit_array);
    }
  }
}


uint32_t TabletReader::TotalRowCount() const {
  boost::optional<BitArray> bit_array = GetBitArray(ReadStoreKey::AllRowsKey());
  if (bit_array) {
    return bit_array->numberOfOnes();
  } else {
    return 0;
  }
}

uint32_t TabletReader::CountExactByColumn(column_t column, const std::string &value) const {
  rocksdb::Status status;
  ReadStoreKey key{column, value};
  std::string result;

  status = db_->Get(env_->getReadStoreReadOptions(), key.ToSlice(), &result);
  if (status.IsNotFound()) {
    return 0;
  } else if (!status.ok()) {
    throw TabletLevelDbException{status};
  }

  BitArray bitArray;
  bitArray.readStream(result.data(), result.size());

  return bitArray.numberOfOnes();
}

void TabletReader::QueryExactByColumn(column_t column, const std::string &value, std::function<void(const Record &)> callable) const {
  rocksdb::Status status;
  ReadStoreKey key{column, value};
  std::string result;

  status = db_->Get(env_->getReadStoreReadOptions(), key.ToSlice(), &result);
  if (status.IsNotFound()) {
    return;
  } else if (!status.ok()) {
    throw TabletLevelDbException{status};
  }

  BitArray bitArray;
  bitArray.readStream(result.data(), result.size());

  result.clear();
  Record currentRecord;

  bitArray.iterateSetBits([&] (uint64_t pos) {
    rocksdb::Slice newKey = ReadStoreKey::MaterializedRowKey(pos).ToSlice();
    status = db_->Get(
        env_->getReadStoreReadOptions(),
        newKey,
        &result
    );
    if (status.IsNotFound()) {
      return;
    }
    if (!status.ok()) {
      throw TabletLevelDbException{status};
    }

    currentRecord.ParseFromString(result);
    callable(currentRecord);
  });
}

uint32_t TabletReader::CountWhere(const Query_Predicate &predicate) const {
  return WhereClause(predicate).numberOfOnes();
}

BitArray TabletReader::WhereClause(const Query_Predicate &predicate) const {
  switch (predicate.type()) {
    case Query_Predicate_PredicateType_AND: {
      BitArray result, current;

      result = WhereClause(predicate.sub_predicates(0));

      std::for_each(predicate.sub_predicates().begin() + 1, predicate.sub_predicates().end(), [&] (const Query_Predicate & child) {
        BitArray sub_bit_array = WhereClause(child);
        result.logicaland(sub_bit_array, current);
        result.swap(current);
      });
      return result;
    }
    case Query_Predicate_PredicateType_OR: {
      BitArray result, current;
      std::for_each(predicate.sub_predicates().begin(), predicate.sub_predicates().end(), [&] (const Query_Predicate & child) {
        BitArray sub_bit_array = WhereClause(child);
        result.logicalor(sub_bit_array, current);
        result.swap(current);
      });
      return result;
    }
    case Query_Predicate_PredicateType_NODE:
      return dullahan::query::kPredicatesByOperator[predicate.operator_()](*this, predicate);
  }
  throw std::invalid_argument{"Shouldn't get here"};
}

const TableSchema & TabletReader::table_schema() const {
  return tablet_metadata_.table_metadata();
}


} // namespace dullahan