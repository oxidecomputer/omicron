#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#:
#: name = "build-and-test (ubuntu-22.04)"
#: variety = "basic"
#: target = "ubuntu-22.04-large"
#: rust_toolchain = true
#: output_rules = [
#:	"%/work/*",
#:	"%/work/oxidecomputer/omicron/target/nextest/ci/junit.xml",
#:	"=/tmp/nextest-run-archive.zip",
#:	"=/tmp/nextest-chrome-trace.json",
#:	"%/var/tmp/omicron_tmp/**/*",
#:	"!/var/tmp/omicron_tmp/crdb-base*",
#:	"!/var/tmp/omicron_tmp/rustc*",
#: ]
#:
#: [[publish]]
#: series = "junit-linux"
#: name = "junit.xml"
#: from_output = "/work/oxidecomputer/omicron/target/nextest/ci/junit.xml"
#:
#: [[publish]]
#: series = "junit-linux"
#: name = "environment.json"
#: from_output = "/work/environment.json"
#:
#: [[publish]]
#: series = "build-info-linux"
#: name = "cargo-build-analysis.jsonl"
#: from_output = "/work/cargo-build-analysis.jsonl"
#:
#: [[publish]]
#: series = "nextest-recording-linux"
#: name = "nextest-run-archive.zip"
#: from_output = "/tmp/nextest-run-archive.zip"
#:
#: [[publish]]
#: series = "nextest-recording-linux"
#: name = "nextest-chrome-trace.json"
#: from_output = "/tmp/nextest-chrome-trace.json"

sudo apt-get install -y jq
exec .github/buildomat/build-and-test.sh linux
