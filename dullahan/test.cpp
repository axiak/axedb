#include <iostream>
#include <csv.h>
#include "env.hpp"
#include "protos/dullahan.pb.h"
#include "readstore/tabletwriter.hpp"
#include "readstore/query/tabletreader.hpp"

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

  TabletMetadata tabletMetadata;
  tabletMetadata.set_timestamp_start(0);
  tabletMetadata.set_timestamp_stop(3);

  try {
    Env *env = Env::getEnv();
    StringBuffer stringBuffer;

    env->setDataDir("/home/axiak/BigDocuments/dullahan-data");

    TabletReader tabletReader(env, tabletMetadata);

    std::cout << argc << std::endl;

    if (argc > 1) {
      int64_t campaignId = std::stol(argv[1]);
      std::string value{};
      value.append(reinterpret_cast<const char *>(&campaignId), sizeof(int64_t));
      std::cout << "Looking for campaign id " << campaignId << std::endl;
      /*
      tabletReader.QueryExactByColumn(6, value, [&total](const std::string & id) {
        std::cout << "id: " << id << std::endl;
      });
      */
      std::cout << "Total: " << tabletReader.CountExactByColumn(6, value) << std::endl;

    }


    return 0;

    TabletWriter tablet(env, tabletMetadata);

    io::CSVReader<8> in("/home/axiak/BigDocuments/emailevents.csv");
    in.read_header(io::ignore_extra_column, "id", "created", "dateint", "type", "portalId", "appId", "campaignId", "recipient");

    std::string id, recipient, type;
    long created, campaignId;
    int dateint, portalId, appId;

    long total = 0;

    vector<std::string> valueBuffer;

    std::vector<models::Record> records;
    records.reserve(FLUSH_SIZE);

    while (in.read_row(id, created, dateint, type, portalId, appId, campaignId, recipient)) {

      models::Record record;
      record.set_timestamp(created);
      std::string & next = stringBuffer.nextString();
      next.append(id);
      record.set_id(next);

      uint32_t currentColumn = 0;

      models::Record_KeyValue * keyValue = record.add_values();
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
    /*
    const int max_j = 256;
    const int max_k = 20;

    for (int k = 0; k < max_k; ++k) {
      std::vector<models::Record> records;
      for (int j = 0; j < max_j; ++j) {
        models::Record record;
        record.set_timestamp(5);
        models::Record_KeyValue * keyValue = record.add_values();
        keyValue->set_column(23389);
        std::string data;
        data.resize(10);
        for (int i = 0; i < 10; ++i) {
          data[i] = 'a' + ((i + j + k) % 26);
        }
        keyValue->set_value(data);

        records.push_back(record);
      };

      tablet.FlushRecords(records.begin(), records.end());
      tablet.Compact();
    }
    */
  } catch (TabletLevelDbException e) {
    std::cerr << "Exception! " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
