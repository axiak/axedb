#!/bin/bash
cd $(dirname "$0")/dullahan/protos

protoc *.proto --cpp_out=.
