#!/bin/sh

set -ex

cd ./deps/libmdbx
git fetch --tags --force --prune
make lib
