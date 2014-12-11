#include <gtest/gtest.h>

#include <stdlib.h>

#include <ewah.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <unordered_set>
#include <unordered_map>

#include "../readstore/tablet.hpp"
#include "../readstore/query/tabletreader.hpp"

namespace dullahan {

class ReadStoreTest : public ::testing::Test {
protected:
  virtual void SetUp() {
    char * tempFolder = (char *)malloc(255);
    char const *tmpdir = getenv("TMPDIR");

    if (tmpdir == 0) {
      tmpdir = "/tmp";
    }

    assert(strlen(tmpdir) < 255 - 14);

    strcpy(tempFolder, tmpdir);
    strcat(tempFolder, "/tabletXXXXXX");


    if (!mkdtemp(tempFolder)) {
      throw std::runtime_error{"foo"};
    }

    tempDir = tempFolder;

    Env *env = Env::getEnv();
    env->setDataDir(tempDir);

    tabletWriter = std::make_shared<TabletWriter>(env, 0L, 1L);
  }

  virtual void TearDown() {
    tabletWriter.reset();
    std::string command{"rm -rf "};
    command.append(tempDir);
    system(command.data());
  }

  std::string tempDir;
  std::shared_ptr<TabletWriter> tabletWriter;
};

TEST_F(ReadStoreTest, AddAndQueryItems) {
  boost::uuids::random_generator gen;

  vector<Record> records;
  std::unordered_multimap<int, std::string> byValue;

  for (int i = 0; i < 100; ++i) {
    Record record;
    std::string id = boost::uuids::to_string(gen());
    record.set_id(id);
    record.set_timestamp(5);
    Record_KeyValue * keyValue = record.add_values();
    keyValue->set_column(0);
    uint32_t valueInt = (uint32_t)((7 * i) % 5);
    std::string value{};
    value.append(reinterpret_cast<const char *>(&valueInt), sizeof(uint32_t));
    keyValue->set_value(value);

    records.push_back(record);
    byValue.emplace(valueInt, id);

    if (records.size() >= 32) {
      tabletWriter->flushRecords(records);
      records.clear();
    }
  }

  if (!records.empty()) {
    tabletWriter->flushRecords(records);
  }

  tabletWriter->compact();
  tabletWriter.reset();

  Env * env = Env::getEnv();

  TabletReader tabletReader{env, 0L, 1L};

  std::unordered_set<std::string> expected{}, actual{};

  uint32_t valueToFind = 1;
  auto range = byValue.equal_range(valueToFind);

  std::for_each(range.first, range.second,
  [&expected](std::unordered_multimap<int, std::string>::value_type & entry) {
    expected.emplace(entry.second);
  });

  std::string valueBytes{};
  valueBytes.append(reinterpret_cast<const char *>(&valueToFind), sizeof(uint32_t));
  tabletReader.queryExactByColumn(0, valueBytes, [&actual] (const std::string & id) {
    actual.emplace(id);
  });

  ASSERT_EQ(expected, actual) << "IDs queried for are as expected.";
}

}