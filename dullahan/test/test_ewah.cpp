#include <gtest/gtest.h>

#include <ewah.h>

namespace {
inline uint32_t padToWord(uint32_t input) {
  return input + EWAHBoolArray<uint32_t>::padToWord(input);
}
}

class BitArrayTest : public ::testing::Test {
protected:
  virtual void SetUp() {
    oddBitArray.set(1);
    oddBitArray.set(3);
    oddBitArray.set(5);
    oddBitArray.set(7);
    oddBitArray.set(9);
    oddBitArray.set(11);
    oddBitArray.set(13);
    oddBitArray.set(15);
    evenBitArray.set(0);
    evenBitArray.set(2);
    evenBitArray.set(4);
    evenBitArray.set(6);
    evenBitArray.set(8);
    evenBitArray.set(10);
    evenBitArray.set(12);
    evenBitArray.set(14);

    highestOddBit = 15;
    highestEvenBit = 14;
    
    oddBitArray.padWithZeroes(padToWord(highestOddBit));
    evenBitArray.padWithZeroes(padToWord(highestEvenBit));
  }

  // virtual void TearDown() {}
  uint32_t highestOddBit;
  uint32_t highestEvenBit;
  EWAHBoolArray<uint32_t> oddBitArray;
  EWAHBoolArray<uint32_t> evenBitArray;
};

TEST_F(BitArrayTest, SimpleSerialization) {
  std::string output;
  oddBitArray.appendToString(&output, 0);

  ASSERT_EQ(20, output.size()) << "Stream serialized size.";
  const char * expected = "\x00\x00\x00\x00" // offset
                          "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
                          "\x00\x00\x02\x00\252\252\x00\x00"; // 8 characters of data

  ASSERT_EQ(0, strncmp(expected, output.data(), output.size())) << "Stream is serialized properly.";
}

TEST_F(BitArrayTest, AppendingSerialization) {
  std::string output;
  oddBitArray.appendToString(&output, 0);

  const int gap = 128;

  evenBitArray.appendToString(&output, padToWord(gap + highestOddBit));

  ASSERT_EQ(40, output.size()) << "Stream serialized size.";
  const char * expected =
      "\x00\x00\x00\x00" // offset
      "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
      "\x20\x00\x00\x00" // bit size
      "\x00\x00\x02\x00\252\252\x00\x00" // 8 characters of data
      "\220\x00\x00\x00" // offset : 144 = 128 + 16
      "\x02\x00\x00\x00" // buffer size, 2
      "\x20\x00\x00\x00" // bit size
      "\x00\x00\x02\x00\x55\x55\x00\x00" // 8 characters of data
      ; // trailing buffer size: 2

  ASSERT_EQ(0, strncmp(expected, output.data(), output.size())) << "Stream is serialized properly.";
}

TEST_F(BitArrayTest, AppendingSerializationNoGap) {
  std::string output;
  oddBitArray.appendToString(&output, 0);

  const int gap = 0;

  evenBitArray.appendToString(&output, padToWord(gap + highestOddBit));


  ASSERT_EQ(40, output.size()) << "Stream serialized size.";
  const char * expected =
      "\x00\x00\x00\x00" // offset
      "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
      "\x20\x00\x00\x00" // bit size
      "\x00\x00\x02\x00\252\252\x00\x00" // 8 characters of data
      "\x00\x00\x00\x00" // offset
      "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
      "\x00\x00\x02\x00\x55\x55\x00\x00" // 8 characters of data
      ;

  ASSERT_EQ(0, strncmp(expected, output.data(), output.size())) << "Stream is serialized properly.";
}



TEST_F(BitArrayTest, ConcatSerialization) {
  std::string output1, output2;

  const int gap = 128;

  oddBitArray.appendToString(&output1, 0);
  evenBitArray.appendToString(&output2, padToWord(gap + highestOddBit));

  EWAHBoolArray<uint32_t>::concatStreams(&output1, output2.data(), output2.size());


  ASSERT_EQ(40, output1.size()) << "Stream serialized size.";
  const char * expected =
      "\x00\x00\x00\x00" // offset
      "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
      "\x20\x00\x00\x00" // bit size
      "\x00\x00\x02\x00\252\252\x00\x00" // 8 characters of data
      "\240\x00\x00\x00" // offset : 160 = 128 + 16 rounded up to 32
      "\x02\x00\x00\x00" // buffer size, 2
      "\x00\x00\x02\x00\x55\x55\x00\x00" // 8 characters of data
      ;

  ASSERT_EQ(0, strncmp(expected, output1.data(), output1.size())) << "Stream is serialized properly.";
}

TEST_F(BitArrayTest, ConcatSerializationNoGap) {
  std::string output1, output2;

  const int gap = 0;

  oddBitArray.appendToString(&output1, 0);
  evenBitArray.appendToString(&output2, padToWord(gap + highestOddBit));

  EWAHBoolArray<uint32_t>::concatStreams(&output1, output2.data(), output2.size());


  ASSERT_EQ(40, output1.size()) << "Stream serialized size.";
  const char * expected =
      "\x00\x00\x00\x00" // offset
      "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
      "\x20\x00\x00\x00" // bit size
      "\x00\x00\x02\x00\252\252\x00\x00" // 8 characters of data
      "\x00\x00\x00\x00" // offset
      "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
      "\x20\x00\x00\x00" // bit size
      "\x00\x00\x02\x00\x55\x55\x00\x00" // 8 characters of data
      ;

  ASSERT_EQ(0, strncmp(expected, output1.data(), output1.size())) << "Stream is serialized properly.";
}

void showArray(const EWAHBoolArray<uint32_t> & array) {
  const vector<uint32_t> & buffer = array.getBuffer();
  std::for_each(buffer.begin(), buffer.end(), [] (uint32_t item) {
    printf("%08x", item);
    printf("\n");
  });
  printf("\n\n");
}


TEST_F(BitArrayTest, DeserializeGap) {

  const char * deserialized =
          "\x00\x00\x00\x00" // offset
          "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
          "\x20\x00\x00\x00" // bit size
          "\x00\x00\x02\x00\252\252\x00\x00" // 8 characters of data
          "\240\x00\x00\x00" // offset
          "\x02\x00\x00\x00" // buffer size, 2 = 64 bits = 8 characters
          "\x20\x00\x00\x00" // bit size
          "\x00\x00\x02\x00\x55\x55\x00\x00" // 8 characters of data
  ;

  const size_t length = 40;

  EWAHBoolArray<uint32_t> bitarray{};
  bitarray.readStream(deserialized, length);

  ASSERT_TRUE(bitarray.get(1));
  ASSERT_TRUE(bitarray.get(3));
  ASSERT_TRUE(bitarray.get(5));
  ASSERT_TRUE(bitarray.get(7));
  ASSERT_TRUE(bitarray.get(9));
  ASSERT_TRUE(bitarray.get(11));
  ASSERT_TRUE(bitarray.get(13));
  ASSERT_TRUE(bitarray.get(15));

  ASSERT_FALSE(bitarray.get(0));
  ASSERT_FALSE(bitarray.get(2));
  ASSERT_FALSE(bitarray.get(4));
  ASSERT_FALSE(bitarray.get(6));
  ASSERT_FALSE(bitarray.get(8));
  ASSERT_FALSE(bitarray.get(10));
  ASSERT_FALSE(bitarray.get(12));
  ASSERT_FALSE(bitarray.get(14));

  uint64_t offset = 160;

  ASSERT_FALSE(bitarray.get(offset + 1));
  ASSERT_FALSE(bitarray.get(offset + 3));
  ASSERT_FALSE(bitarray.get(offset + 5));
  ASSERT_FALSE(bitarray.get(offset + 7));
  ASSERT_FALSE(bitarray.get(offset + 9));
  ASSERT_FALSE(bitarray.get(offset + 11));
  ASSERT_FALSE(bitarray.get(offset + 13));
  ASSERT_FALSE(bitarray.get(offset + 15));

  ASSERT_TRUE(bitarray.get(offset + 0));
  ASSERT_TRUE(bitarray.get(offset + 2));
  ASSERT_TRUE(bitarray.get(offset + 4));
  ASSERT_TRUE(bitarray.get(offset + 6));
  ASSERT_TRUE(bitarray.get(offset + 8));
  ASSERT_TRUE(bitarray.get(offset + 10));
  ASSERT_TRUE(bitarray.get(offset + 12));
  ASSERT_TRUE(bitarray.get(offset + 14));

  for (uint64_t i = 16; i < offset; ++i) {
    ASSERT_FALSE(bitarray.get(i));
  }
}



TEST_F(BitArrayTest, DeserializeNoGap) {
  const char * deserialized =
      "\x00\x00\x00\x00" // offset
      "\x04\x00\x00\x00" // buffer size, 4
      "\x40\x00\x00\x00" // bit size
      "\x00\x00\x02\x00\252\252\x00\x00" // 8 characters of data
      "\x00\x00\x02\x00\x55\x55\x00\x00" // 8 characters of data
      ;

  const size_t length = 28;

  EWAHBoolArray<uint32_t>bitarray;

  bitarray.readStream(deserialized, length);

  ASSERT_TRUE(bitarray.get(1));
  ASSERT_TRUE(bitarray.get(3));
  ASSERT_TRUE(bitarray.get(5));
  ASSERT_TRUE(bitarray.get(7));
  ASSERT_TRUE(bitarray.get(9));
  ASSERT_TRUE(bitarray.get(11));
  ASSERT_TRUE(bitarray.get(13));
  ASSERT_TRUE(bitarray.get(15));

  ASSERT_FALSE(bitarray.get(0));
  ASSERT_FALSE(bitarray.get(2));
  ASSERT_FALSE(bitarray.get(4));
  ASSERT_FALSE(bitarray.get(6));
  ASSERT_FALSE(bitarray.get(8));
  ASSERT_FALSE(bitarray.get(10));
  ASSERT_FALSE(bitarray.get(12));
  ASSERT_FALSE(bitarray.get(14));

  uint64_t offset = 32;

  ASSERT_FALSE(bitarray.get(offset + 1));
  ASSERT_FALSE(bitarray.get(offset + 3));
  ASSERT_FALSE(bitarray.get(offset + 5));
  ASSERT_FALSE(bitarray.get(offset + 7));
  ASSERT_FALSE(bitarray.get(offset + 9));
  ASSERT_FALSE(bitarray.get(offset + 11));
  ASSERT_FALSE(bitarray.get(offset + 13));
  ASSERT_FALSE(bitarray.get(offset + 15));

  ASSERT_TRUE(bitarray.get(offset + 0));
  ASSERT_TRUE(bitarray.get(offset + 2));
  ASSERT_TRUE(bitarray.get(offset + 4));
  ASSERT_TRUE(bitarray.get(offset + 6));
  ASSERT_TRUE(bitarray.get(offset + 8));
  ASSERT_TRUE(bitarray.get(offset + 10));
  ASSERT_TRUE(bitarray.get(offset + 12));
  ASSERT_TRUE(bitarray.get(offset + 14));

  for (uint64_t i = 16; i < offset; ++i) {
    ASSERT_FALSE(bitarray.get(i));
  }
}
