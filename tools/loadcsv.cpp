#include <iostream>
#include <csv.h>
#include "../dullahan/env.hpp"
#include "../dullahan/protos/dullahan.pb.h"
#include "../dullahan/readstore/tabletwriter.hpp"
#include "../dullahan/readstore/query/tabletreader.hpp"

using namespace dullahan;

const int FLUSH_SIZE = 64;

namespace {
class StringBuffer {
public:
  StringBuffer(): currentIndex{0}, buffer{} {}

  std::string & nextString() {
    if (currentIndex == buffer.size()) {
      buffer.emplace_back();
    } else if (currentIndex > buffer.size()) {
      throw std::exception{};
    }
    std::string & result = buffer[currentIndex++];
    result.clear();
    return result;
  }
  void clear() {
    currentIndex = 0;
  }
private:
  size_t currentIndex;
  vector<std::string> buffer;
};
}


inline std::string & setInt(std::string & target, const int * ref) {
  target.append(reinterpret_cast<const char *>(ref), sizeof(int));
  return target;
}

inline std::string & setInt(std::string & target, const long * ref) {
  target.append(reinterpret_cast<const char *>(ref), sizeof(long));
  return target;
}

inline std::string & setString(std::string & target, const std::string & ref) {
  target.append(ref);
  return target;
}

int main(int argc, char ** argv) {
  StringBuffer metadataBuffer, stringBuffer;

  TabletMetadata tabletMetadata;
  tabletMetadata.set_timestamp_start(3);
  tabletMetadata.set_timestamp_stop(6);
  Env *env = Env::getEnv();

  TableSchema * tableSchema = tabletMetadata.mutable_table_metadata();

  TableSchema_Column* column = tableSchema->add_columns();
  column->set_name(setString(metadataBuffer.nextString(), "id"));
  column->set_type(TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_STRING);

  column = tableSchema->add_columns();
  column->set_name(setString(metadataBuffer.nextString(), "created"));
  column->set_type(TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_BIGINT);

  column = tableSchema->add_columns();
  column->set_name(setString(metadataBuffer.nextString(), "dateint"));
  column->set_type(TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_INTEGER);

  column = tableSchema->add_columns();
  column->set_name(setString(metadataBuffer.nextString(), "type"));
  column->set_type(TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_STRING);

  column = tableSchema->add_columns();
  column->set_name(setString(metadataBuffer.nextString(), "portalId"));
  column->set_type(TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_INTEGER);

  column = tableSchema->add_columns();
  column->set_name(setString(metadataBuffer.nextString(), "appId"));
  column->set_type(TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_INTEGER);

  column = tableSchema->add_columns();
  column->set_name(setString(metadataBuffer.nextString(), "campaignId"));
  column->set_type(TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_BIGINT);

  column = tableSchema->add_columns();
  column->set_name(setString(metadataBuffer.nextString(), "recipient"));
  column->set_type(TableSchema_Column_ColumnType::TableSchema_Column_ColumnType_STRING);


  env->setDataDir("/home/axiak/BigDocuments/dullahan-data");

  TabletWriter tablet(env, tabletMetadata);

  io::CSVReader<8> in("/home/axiak/BigDocuments/emailevents.csv");
  in.read_header(io::ignore_extra_column, "id", "created", "dateint", "type", "portalId", "appId", "campaignId", "recipient");

  std::string id, recipient, type;
  long created, campaignId;
  int dateint, portalId, appId;

  long total = 0;

  vector <std::string> valueBuffer;

  std::vector<models::Record> records;
  records.reserve(FLUSH_SIZE);

  while (in.read_row(id, created, dateint, type, portalId, appId, campaignId, recipient)) {

    models::Record record;
    record.set_timestamp(created);
    std::string &next = stringBuffer.nextString();
    next.append(id);
    record.set_id(next);

    uint32_t currentColumn = 0;

    models::Record_KeyValue *keyValue = record.add_values();
    keyValue->set_column(currentColumn++);
    keyValue->set_value(next);

    next = stringBuffer.nextString();
    keyValue = record.add_values();
    keyValue->set_column(currentColumn++);
    keyValue->set_value(setInt(next, &created));

    next = stringBuffer.nextString();
    keyValue = record.add_values();
    keyValue->set_column(currentColumn++);
    keyValue->set_value(setInt(next, &dateint));

    next = stringBuffer.nextString();
    keyValue = record.add_values();
    keyValue->set_column(currentColumn++);
    keyValue->set_value(setString(next, type));

    next = stringBuffer.nextString();
    keyValue = record.add_values();
    keyValue->set_column(currentColumn++);
    keyValue->set_value(setInt(next, &portalId));

    next = stringBuffer.nextString();
    keyValue = record.add_values();
    keyValue->set_column(currentColumn++);
    keyValue->set_value(setInt(next, &appId));

    next = stringBuffer.nextString();
    keyValue = record.add_values();
    keyValue->set_column(currentColumn++);
    keyValue->set_value(setInt(next, &campaignId));

    next = stringBuffer.nextString();
    keyValue = record.add_values();
    keyValue->set_column(currentColumn++);
    keyValue->set_value(setString(next, recipient));

    records.push_back(record);

    if (records.size() == FLUSH_SIZE) {
      tablet.FlushRecords(records);
      records.clear();
      stringBuffer.clear();
    }

    if (++total % 1000 == 0) {
      std::cout << "Completed row " << total << std::endl;
    }

  }

  if (!records.empty()) {
    tablet.FlushRecords(records);
    records.clear();
    stringBuffer.clear();
  }

  tablet.Compact();
  std::cout << "total: " << total << std::endl;

  return 0;
}