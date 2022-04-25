#!/bin/sh

set -ex

git clone -b v0.11.7 https://gitflic.ru/project/erthink/libmdbx.git ./deps/libmdbx
cd ./deps/libmdbx
git fetch --tags --force --prune
make lib
