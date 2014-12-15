cmake_minimum_required(VERSION 2.8.4)
project(dullahan)

include(ProcessorCount)

ProcessorCount(PROCESSOR_COUNT)

include_directories(
        3rdparty/smhasher
        3rdparty/EWAHBoolArray/headers
        3rdparty/fast-cpp-csv-parser
        3rdparty/rocksdb/include
)

set(CMAKE_CXX_FLAGS  "-pedantic -pthread -Wall -std=c++11")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DOS_LINUX")
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

set(SOURCE_FILES
        dullahan/env.hpp
        dullahan/env.cpp
        dullahan/dullahan.hpp

        dullahan/utils/bytes.hpp
        dullahan/utils/utils.hpp

        dullahan/readstore/tablet.hpp
        dullahan/readstore/tablet.cpp
        dullahan/readstore/tabletwriter.cpp
        dullahan/readstore/tabletwriter.hpp
        dullahan/readstore/models/base.hpp
        dullahan/readstore/models/key.hpp
        dullahan/readstore/models/key.cpp
        dullahan/readstore/models/value.hpp
        dullahan/readstore/models/value.cpp
        dullahan/readstore/rocksdb/ewahmerge.hpp
        dullahan/readstore/rocksdb/ewahmerge.cpp
        dullahan/readstore/rocksdb/columnordering.cpp
        dullahan/readstore/rocksdb/columnordering.hpp
        dullahan/readstore/query/tabletreader.cpp
        dullahan/readstore/query/tabletreader.hpp
        dullahan/readstore/query/predicate_operators.cpp

        dullahan/protos/dullahan.pb.h
        dullahan/protos/dullahan.pb.cc
        dullahan/protos/utils.hpp
        dullahan/protos/utils.cpp

        3rdparty/EWAHBoolArray/headers/ewahutil.h
        3rdparty/EWAHBoolArray/headers/boolarray.h
        3rdparty/EWAHBoolArray/headers/runninglengthword.h
        3rdparty/EWAHBoolArray/headers/ewah.h

        3rdparty/smhasher/MurmurHash3.cpp
        3rdparty/smhasher/MurmurHash3.h

        3rdparty/fast-cpp-csv-parser/csv.h
)


# Enable ExternalProject CMake module
include(ExternalProject)

# Set the build type if it isn't already
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Set default ExternalProject root directory
set_directory_properties(PROPERTIES EP_PREFIX ${CMAKE_BINARY_DIR}/ThirdParty)

# Add gtest
ExternalProject_Add(
    googletest
    GIT_REPOSITORY git@github.com:axiak/googletest.git
    GIT_TAG f26e19a0a5162a0914b4c0e33429c1e8aa6481af
    TIMEOUT 10
    # Force separate output paths for debug and release builds to allow easy
    # identification of correct lib in subsequent TARGET_LINK_LIBRARIES commands
    CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG:PATH=DebugLibs
               -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE:PATH=ReleaseLibs
               -Dgtest_force_shared_crt=ON
    # Disable install step
    INSTALL_COMMAND ""
    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON)

# Add rocksdb
ExternalProject_Add(
    rocksdb
    GIT_REPOSITORY git@github.com:axiak/rocksdb.git
    GIT_TAG master
    TIMEOUT 30
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE ON
    BUILD_COMMAND make -j${PROCESSOR_COUNT} static_lib
    # Disable
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_BUILD ON
)

# Add json2pb
ExternalProject_Add(
    json2pb
    GIT_REPOSITORY git@github.com:axiak/json2pb.git
    GIT_TAG master
    TIMEOUT 30
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE ON
    BUILD_COMMAND make libjson2pb.so
    # Disable
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_BUILD ON
)

ExternalProject_Get_Property(googletest binary_dir)

SET(GTEST_DIR ${binary_dir})

ExternalProject_Get_Property(rocksdb source_dir)

SET(ROCKSDB_DIR ${source_dir})


ExternalProject_Get_Property(json2pb source_dir)

SET(JSON2PB_DIR ${source_dir})


add_executable(dullahan_test
    #./dullahan/test/test_readstore.cpp
    ./dullahan/test/test_all.cpp
    ${SOURCE_FILES}
)

add_executable(dullahan_query
    ./tools/query.cpp
    ${SOURCE_FILES}
)

add_executable(tablet_load_csv
    ./tools/loadcsv.cpp
    ${SOURCE_FILES}
)



add_dependencies(dullahan_test googletest rocksdb)
add_dependencies(dullahan_query rocksdb json2pb)
add_dependencies(tablet_load_csv rocksdb)

# External include directories
ExternalProject_Get_Property(googletest source_dir)
include_directories(${source_dir}/include)

ExternalProject_Get_Property(rocksdb source_dir)
include_directories(${source_dir}/include)

include_directories(${JSON2PB_DIR})

set(SHARED_LIBRARIES
    ${ROCKSDB_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}rocksdb.a
    ${JSON2PB_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}json2pb.so
    protobuf jemalloc lz4 snappy z)

target_link_libraries(
    dullahan_query
    ${SHARED_LIBRARIES}
)

target_link_libraries(
    tablet_load_csv
    ${SHARED_LIBRARIES}
)

target_link_libraries(
    dullahan_test
    debug ${GTEST_DIR}/DebugLibs/${CMAKE_FIND_LIBRARY_PREFIXES}gtest.a
    optimized ${GTEST_DIR}/ReleaseLibs/${CMAKE_FIND_LIBRARY_PREFIXES}gtest.a
    ${SHARED_LIBRARIES}
)
