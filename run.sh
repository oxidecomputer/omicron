#!/bin/bash

# wrapper script to run part of the Omicron test suite in a loop under nohup
# with stdout/stderr redirected to a unique path

set -o xtrace

cd "$(dirname ${BASH_SOURCE[0]})"

export TMP=$PWD/tmpdir
export TMPDIR=$TMP
export PATH=$PATH:$PWD/out/clickhouse:$PWD/out/cockroachdb/bin
export PATH=$PWD/debug-bin:$PATH
#export PATH=$PWD/maybefixed-bin/bin:$PATH
id=$$
#exec nohup bash -c "env; cd nexus; pwd; time while time pfexec ../target/debug/deps/test_all-710f55af423d38e5 test_disk_create_disk_that_already_exists_fails ; do :; done" > run-$id.out 2>&1
#exec nohup bash -c "env; cd nexus; pwd; time while time pfexec ../target/debug/deps/test_all-710f55af423d38e5 ; do :; done" > run-$id.out 2>&1

# use the following line to run just the part known to reproduce our problem
exec nohup bash -c "env; time while time cargo test -p omicron-nexus --lib -- --test-threads 6 db::; do :; done" > run-$id.out 2>&1

# use the following line to run the whole test suite in a loop
#exec nohup bash -c "env; time while time cargo test ; do :; done" > run-$id.out 2>&1
