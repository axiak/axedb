#include <iostream>
#include "env.hpp"
#include "protos/dullahan.pb.h"
#include "readstore/tablet.hpp"

using namespace dullahan;


int main() {
  try {
    Env *env = Env::getEnv();

    env->setDataDir("/tmp/test");

    Tablet tablet(env, 0, 3);

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

      tablet.flushRecords(records.begin(), records.end());
      tablet.compact();
    }
  } catch (TabletLevelDbException e) {
    std::cerr << "Exception! " << e.what() << std::endl;
    return 1;
  }
  return 0;
}