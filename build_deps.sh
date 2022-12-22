#!/bin/sh

set -ex

DIRNAME="./deps/libmdbx"

if [ -e $DIRNAME ]; then
    rm -rf $DIRNAME
fi

git clone -b $MDBX_VERSION https://gitflic.ru/project/erthink/libmdbx.git $DIRNAME
cd ./deps/libmdbx
git fetch --tags --force --prune
make lib
