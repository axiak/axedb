#include <gtest/gtest.h>

#include "test_ewah.cpp"
#include "test_readstore.cpp"


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}