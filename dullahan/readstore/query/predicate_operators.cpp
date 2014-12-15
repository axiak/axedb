#include <functional>
#include <stdint.h>
#include <cfloat>

#include <ewah.h>

#include "../models/base.hpp"
#include "../../protos/dullahan.pb.h"
#include "tabletreader.hpp"
#include "../../utils/utils.hpp"


namespace dullahan {
namespace query {
using Predicate = std::function<BitArray(const TabletReader &, const Query_Predicate &)>;
}
}



namespace {
using namespace dullahan;
using namespace dullahan::models;
using namespace dullahan::query;


inline ReadStoreKey BinaryIncrement(const std::string & value, bool decrement=false) {
  std::string incremented{value};
  const char amount = decrement ? (char)-1 : (char)1;
  for (size_t i = incremented.size() - 1; i >= 0; --i) {
    incremented[i] += amount;
    if (incremented[i] != 0) {
      break;
    }
  }
  const std::string incremented_key_value{incremented.data() + sizeof(column_t), incremented.size() - sizeof(column_t)};

  ReadStoreKey key{*reinterpret_cast<const column_t *>(incremented.data()), incremented_key_value};
  return key;
}

template<typename T>
inline ReadStoreKey NumericIncrement(const column_t column, const std::string & value, T amount) {
  const T * ptr = reinterpret_cast<const T *>(value.data());
  T new_value = (*ptr) + amount;

  if ((amount > 0 && new_value < *ptr) ||
      (amount < 0 && new_value > *ptr)) { //overflow. just do binary increment
    return BinaryIncrement(value, amount < 0);
  } else {
    vector<byte> key_value{};
    const byte * new_ptr = reinterpret_cast<const byte *>(&new_value);
    key_value.insert(key_value.end(), new_ptr, new_ptr + sizeof(T));
    ReadStoreKey result{column, std::move(key_value)};
    return result;
  }
}

inline ReadStoreKey IncrementKey(const TabletReader &tablet_reader, const column_t column, const std::string & value, bool decrement=false) {
  if (column >= tablet_reader.table_schema().columns_size()) {
    return BinaryIncrement(value, decrement);
  } else {
    switch (tablet_reader.table_schema().columns(column).type()) {
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_SMALLINT:
        return NumericIncrement<int16_t>(column, value, decrement ? -1 : 1);
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_INTEGER:
        return NumericIncrement<int32_t>(column, value, decrement ? -1 : 1);
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_BIGINT:
        return NumericIncrement<int64_t>(column, value, decrement ? -1 : 1);
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_FLOAT:
        return NumericIncrement<float>(column, value, decrement ? -FLT_EPSILON : FLT_EPSILON);
      case TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_DOUBLE:
        return NumericIncrement<double>(column, value, decrement ? -DBL_EPSILON : DBL_EPSILON);
      default:
        return BinaryIncrement(value, decrement);
    }
  }
}

inline const BitArray RangeQuery(const column_t column, const TabletReader &tablet_reader, const ReadStoreKey &inclusive_start, const ReadStoreKey &exclusive_stop) {
  BitArray result, current;
  tablet_reader.GetBitArrays(column, inclusive_start, exclusive_stop, [&result, &current](BitArray bit_array) {
    result.logicalor(bit_array, current);
    result.swap(current);
  });
  return result;
}

inline const BitArray NegateQuery(const TabletReader &tablet_reader, BitArray bit_array) {
  BitArray all_rows = tablet_reader.GetBitArray(ReadStoreKey::AllRowsKey());
  bit_array.padWithZeroes(tablet_reader.HighestId());
  bit_array.inplace_logicalnot();
  BitArray result;
  bit_array.logicaland(all_rows, result);
  return result;
}

const Predicate kEqual = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  ReadStoreKey key{predicate.column(), predicate.operands(0)};
  return tablet_reader.GetBitArray(key);
};

const Predicate kNotEqual = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  ReadStoreKey key{predicate.column(), predicate.operands(0)};
  BitArray index = tablet_reader.GetBitArray(key);
  return NegateQuery(tablet_reader, index);
};


const Predicate kLess = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  ReadStoreKey stop_row = IncrementKey(tablet_reader, predicate.column(), predicate.operands(0), true);
  return RangeQuery(predicate.column(), tablet_reader, ReadStoreKey::EmptyColumnKey(predicate.column()), stop_row);
};

const Predicate kLessOrEqual = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  ReadStoreKey start_key = ReadStoreKey::EmptyColumnKey(predicate.column());
  ReadStoreKey stop_key{predicate.column(), predicate.operands(0)};
  return RangeQuery(predicate.column(), tablet_reader, start_key, stop_key);
};

const Predicate kGreater = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  return NegateQuery(tablet_reader, kLessOrEqual(tablet_reader, predicate));
};

const Predicate kGreaterOrEqual = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  return NegateQuery(tablet_reader, kLess(tablet_reader, predicate));
};


const Predicate kIn = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  BitArray result, current, bit_array;
  std::for_each(predicate.operands().begin(), predicate.operands().end(), [&](const std::string & operand){
    ReadStoreKey key{predicate.column(), operand};
    bit_array = tablet_reader.GetBitArray(key);
    result.logicalor(bit_array, current);
    result.swap(current);
  });
  return result;
};

const Predicate kNotIn = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  return NegateQuery(tablet_reader, kIn(tablet_reader, predicate));
};

const Predicate kBetween = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  assert(predicate.operands_size() == 2);
  ReadStoreKey start_key{predicate.column(), predicate.operands(0)};
  ReadStoreKey stop_key = IncrementKey(tablet_reader, predicate.column(), predicate.operands(1), false);
  return RangeQuery(predicate.column(), tablet_reader, start_key, stop_key);
};

const Predicate kNotBetween = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  return NegateQuery(tablet_reader, kBetween(tablet_reader, predicate));
};

const Predicate kLike = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) -> BitArray {
  throw std::invalid_argument{"foo"};
};

const Predicate kNotLike = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  return NegateQuery(tablet_reader, kLike(tablet_reader, predicate));
};

const Predicate kIsNotNull = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  return tablet_reader.GetBitArray(ReadStoreKey::AllRowsForColumnKey(predicate.column()));
};

const Predicate kIsNull = [](const TabletReader &tablet_reader, const Query_Predicate &predicate) {
  return NegateQuery(tablet_reader, kIsNotNull(tablet_reader, predicate));
};

} // anonymous namespace

namespace dullahan {
namespace query {

const Predicate kPredicatesByOperator[] = {
    nullptr,
    kEqual,
    kNotEqual,
    kGreater,
    kGreaterOrEqual,
    kLess,
    kLessOrEqual,
    kIn,
    kNotIn,
    kBetween,
    kNotBetween,
    kLike,
    kNotLike,
    kIsNull,
    kIsNotNull
};


/*  enum Operator {
        EQUAL = 1;
        NOT_EQUAL = 2;
        GREATER = 3;
        GREATER_OR_EQUAL = 4;
        LESS = 5;
        LESS_OR_EQUAL = 6;
        IN = 7;
        NOT_IN = 8;
        BETWEEN = 9;
        NOT_BETWEEN = 10;
        LIKE = 11;
        NOT_LIKE = 12;
        IS_NULL = 13;
        IS_NOT_NULL = 14;
    }
 */

}
}