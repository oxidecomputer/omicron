#!/bin/bash

# wrapper script to run part of the Omicron test suite in a loop under nohup
# with stdout/stderr redirected to a unique path

set -o xtrace

cd "$(dirname ${BASH_SOURCE[0]})"

#export TMP=$PWD/tmpdir
#export TMPDIR=$TMP
export PATH=$PATH:$PWD/out/clickhouse:$PWD/out/cockroachdb/bin
#export PATH=$PWD/debug-bin:$PATH
export PATH=$PWD/maybefixed-bin/bin:$PATH
id=$$
exec nohup bash -c "env; time while time cargo test -p omicron-nexus --lib -- db:: ; do :; done" > run-$id.out 2>&1
