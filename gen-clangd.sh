#!/bin/bash

cat << EOF >.clangd
CompileFlags:
  Add:
    - -std=c++20
    - -Wall
    - -I$(pwd)/seastar/include
    - -I$(pwd)/seastar/build/release/gen/src
    - -I$(pwd)/seastar/build/release/gen/include
    - -I$(pwd)/seastar/build/release/include
    - -I$(pwd)/src
    - -DBOOST_NO_CXX98_FUNCTION_BASE
    - -DFMT_SHARED
    - -DSEASTAR_API_LEVEL=7
    - -DSEASTAR_BROKEN_SOURCE_LOCATION
    - -DSEASTAR_LOGGER_COMPILE_TIME_FMT
    - -DSEASTAR_LOGGER_TYPE_STDOUT
    - -DSEASTAR_SCHEDULING_GROUPS_COUNT=16
    - -DSEASTAR_SSTRING
EOF
