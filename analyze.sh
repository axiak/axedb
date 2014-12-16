#!/bin/bash
cd $(dirname "$0")

mkdir -p builds/Analyze
cd builds/Analyze

make clean

cmake -DCMAKE_C_COMPILER=/usr/share/clang/scan-build/ccc-analyzer -DCMAKE_CXX_COMPILER=/usr/share/clang/scan-build/c++-analyzer ../../

scan-build make
