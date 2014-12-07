#!/bin/bash
find ./3rdparty/rocksdb/ | egrep '\.(cc|c|h)$' | grep -v tools | grep -v test | grep -v bench | grep -v example | grep -v /java/ | perl -lne 'print "        $_"'

