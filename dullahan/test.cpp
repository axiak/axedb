#include <iostream>
#include <csv.h>
#include "env.hpp"
#include "protos/dullahan.pb.h"
#include "readstore/tabletwriter.hpp"
#include "readstore/query/tabletreader.hpp"

using namespace dullahan;

int main(int argc, char ** argv) {

  TabletMetadata tabletMetadata;
  tabletMetadata.set_timestamp_start(0);
  tabletMetadata.set_timestamp_stop(3);

  try {
    Env *env = Env::getEnv();
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
  } catch (TabletLevelDbException e) {
    std::cerr << "Exception! " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
