#!/bin/sh

set -ex

DIRNAME="./deps/libmdbx"

if [ -e $DIRNAME ]; then
    rm -rf $DIRNAME
fi

mkdir -p $DIRNAME
wget -P $DIRNAME https://libmdbx.dqdkfa.ru/release/libmdbx-amalgamated-${MDBX_VERSION}.tar.gz
cd ./deps/libmdbx
tar xvzf libmdbx-amalgamated-${MDBX_VERSION}.tar.gz
make lib
