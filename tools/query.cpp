#include <json2pb.h>
#include <iostream>
#include <csv.h>
#include "../dullahan/env.hpp"
#include "../dullahan/protos/dullahan.pb.h"
#include "../dullahan/readstore/tabletwriter.hpp"
#include "../dullahan/readstore/query/tabletreader.hpp"

using namespace dullahan;

int main(int argc, char ** argv) {

  TabletMetadata tabletMetadata;
  tabletMetadata.set_timestamp_start(3);
  tabletMetadata.set_timestamp_stop(6);

  try {
    Env *env = Env::getEnv();
    env->setDataDir("/home/axiak/BigDocuments/dullahan-data");
    TabletReader tabletReader(env, tabletMetadata);

    if (argc > 1) {
      Query_Predicate query_predicate;
      json2pb(query_predicate, argv[1], strlen(argv[1]));


      std::cout << "Running query: " << std::endl;
      query_predicate.PrintDebugString();

      std::cout << "Count: " << tabletReader.CountWhere(query_predicate) << std::endl;

      tabletReader.QueryWhere(query_predicate, 10, [] (const Record & record) {
        std::cout << record.ShortDebugString() << std::endl;
      });

      /*
      value.append(reinterpret_cast<const char *>(&campaignId), sizeof(int64_t));
      std::cout << "Looking for campaign id " << campaignId << std::endl;
      /*
      tabletReader.QueryExactByColumn(6, value, [&total](const std::string & id) {
        std::cout << "id: " << id << std::endl;
      });
      std::cout << "Total: " << tabletReader.CountExactByColumn(6, value) << std::endl;

      std::cout << "Total: " << ta
      */
    } else {
      std::cerr << "Usage: " << argv[0] << " " << "'<json of predicate>'" << std::endl;
    }
    return 0;
  } catch (TabletLevelDbException e) {
    std::cerr << "Exception! " << e.what() << std::endl;
    return 1;
  }
}
