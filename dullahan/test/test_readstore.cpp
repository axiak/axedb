#include <gtest/gtest.h>

#include <stdlib.h>

#include <ewah.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <unordered_set>
#include <unordered_map>

#include "../readstore/query/tabletreader.hpp"
#include "../readstore/tabletwriter.hpp"


namespace dullahan {

TabletMetadata BuildMetadata() {
  TabletMetadata tablet_metadata;
  tablet_metadata.set_timestamp_start(0);
  tablet_metadata.set_timestamp_stop(1);
  TableSchema * schema = tablet_metadata.mutable_table_metadata();
  schema->clear_columns();
  schema->add_columns();
  TableSchema_Column * float_column = schema->add_columns();
  float_column->set_type(TableSchema_Column_ColumnType_FLOAT);
  TableSchema_Column * int_column = schema->add_columns();
  int_column->set_type(TableSchema_Column_ColumnType_INTEGER);
  TableSchema_Column * long_column = schema->add_columns();
  long_column->set_type(TableSchema_Column_ColumnType_BIGINT);
  return tablet_metadata;
}

class ReadStoreTest : public ::testing::Test {
protected:
  virtual void SetUp() {
    tabletMetadata = BuildMetadata();

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

    tabletWriter = std::make_shared<TabletWriter>(env, tabletMetadata);
  }

  virtual void TearDown() {
    tabletWriter.reset();
    std::string command{"rm -rf "};
    command.append(tempDir);
    system(command.data());
  }

  TabletMetadata tabletMetadata;
  std::string tempDir;
  std::shared_ptr<TabletWriter> tabletWriter;
};


TEST_F(ReadStoreTest, FloatOrdering) {
  boost::uuids::random_generator gen;
  vector<Record> records;

  vector<float> test_floats{
      -0.001,
      0.0,
      0.001,
      1,
      std::numeric_limits<float>::max(),
      std::numeric_limits<float>::min(),
      std::numeric_limits<float>::lowest()
  };

  for (float i : test_floats) {
    Record record;
    std::string id = boost::uuids::to_string(gen());
    record.set_id(id);
    record.set_timestamp(5);

    Record_KeyValue * key_value = record.add_values();
    key_value->set_column(1);
    std::string value;
    value.append(reinterpret_cast<const char *>(&i), sizeof(float));
    key_value->set_value(value);

    records.push_back(record);
  }

  tabletWriter->FlushRecords(records);
  tabletWriter->Compact();
  tabletWriter.reset();

  Env * env = Env::getEnv();

  TabletReader tablet_reader(env, tabletMetadata);

  vector<float> actual;

  tablet_reader.OrderBy(1, [&] (const Record & record) {
    float result = *reinterpret_cast<const float *>(record.values(0).value().data());
    actual.emplace_back(result);
  });

  vector<float> expected{
      std::numeric_limits<float>::lowest(),
      -0.001,
      0.0,
      std::numeric_limits<float>::min(),
      0.001,
      1,
      std::numeric_limits<float>::max()
  };

  ASSERT_EQ(expected, actual) << "Floats are ordered properly.";
}



TEST_F(ReadStoreTest, IntOrdering) {
  boost::uuids::random_generator gen;
  vector<Record> records;

  vector<int> test_values{
      -100,
      1,
      -1,
      0,
      2,
      std::numeric_limits<int>::max(),
      std::numeric_limits<int>::min(),
      std::numeric_limits<int>::lowest()
  };

  for (int i : test_values) {
    Record record;
    std::string id = boost::uuids::to_string(gen());
    record.set_id(id);
    record.set_timestamp(5);

    Record_KeyValue * key_value = record.add_values();
    key_value->set_column(2);
    std::string value;
    value.append(reinterpret_cast<const char *>(&i), sizeof(int));
    key_value->set_value(value);

    records.push_back(record);
  }

  tabletWriter->FlushRecords(records);
  tabletWriter->Compact();
  tabletWriter.reset();

  Env * env = Env::getEnv();

  TabletReader tablet_reader(env, tabletMetadata);

  vector<int> actual;

  tablet_reader.OrderBy(2, [&] (const Record & record) {
    int result = *reinterpret_cast<const int *>(record.values(0).value().data());
    actual.emplace_back(result);
  });

  vector<int> expected{
      std::numeric_limits<int>::lowest(),
      std::numeric_limits<int>::min(),
      -100,
      -1,
      0,
      1,
      2,
      std::numeric_limits<int>::max()
  };

  ASSERT_EQ(expected, actual) << "Integers are ordered properly.";
}


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
      tabletWriter->FlushRecords(records);
      records.clear();
    }
  }

  if (!records.empty()) {
    tabletWriter->FlushRecords(records);
  }

  tabletWriter->Compact();
  tabletWriter.reset();

  Env * env = Env::getEnv();

  TabletReader tabletReader(env, tabletMetadata);

  std::unordered_set<std::string> expected{}, actual{};

  uint32_t valueToFind = 1;
  auto range = byValue.equal_range(valueToFind);

  std::for_each(range.first, range.second,
  [&expected](std::unordered_multimap<int, std::string>::value_type & entry) {
    expected.emplace(entry.second);
  });

  std::string valueBytes{};
  valueBytes.append(reinterpret_cast<const char *>(&valueToFind), sizeof(uint32_t));
  tabletReader.QueryExactByColumn(0, valueBytes, [&actual] (const Record & record) {
    actual.emplace(record.id());
  });

  ASSERT_EQ(expected, actual) << "IDs queried for are as expected.";
}

}