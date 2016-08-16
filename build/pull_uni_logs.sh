#! /usr/bin/env sh

pushd .
cd ${0%/*}
cd ..

python ./build/pull_logs.py

popd
