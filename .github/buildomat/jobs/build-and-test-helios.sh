#!/bin/bash
#:
#: name = "build-and-test (helios)"
#: variety = "basic"
#: target = "helios-3.0-32c256gb"
#: rust_toolchain = true
#: output_rules = [
#:	"%/work/*",
#:	"%/work/oxidecomputer/omicron/target/nextest/ci/junit.xml",
#:	"%/work/oxidecomputer/omicron/target/live-tests-archive.tgz",
#:	"=/tmp/nextest-run-archive.zip",
#:	"=/tmp/nextest-chrome-trace.json",
#:	"%/var/tmp/omicron_tmp/**/*",
#:	"!/var/tmp/omicron_tmp/crdb-base*",
#:	"!/var/tmp/omicron_tmp/rustc*",
#: ]
#: access_repos = [
#:	"oxidecomputer/dendrite"
#: ]
#:
#: [[publish]]
#: series = "junit-helios"
#: name = "junit.xml"
#: from_output = "/work/oxidecomputer/omicron/target/nextest/ci/junit.xml"
#:
#: [[publish]]
#: series = "junit-helios"
#: name = "environment.json"
#: from_output = "/work/environment.json"
#:
#: [[publish]]
#: series = "build-info-helios"
#: name = "cargo-build-analysis.jsonl"
#: from_output = "/work/cargo-build-analysis.jsonl"
#:
#: [[publish]]
#: series = "live-tests"
#: name = "live-tests-archive.tgz"
#: from_output = "/work/oxidecomputer/omicron/target/live-tests-archive.tgz"
#:
#: [[publish]]
#: series = "nextest-recording-helios"
#: name = "nextest-run-archive.zip"
#: from_output = "/tmp/nextest-run-archive.zip"
#:
#: [[publish]]
#: series = "nextest-recording-helios"
#: name = "nextest-chrome-trace.json"
#: from_output = "/tmp/nextest-chrome-trace.json"

exec .github/buildomat/build-and-test.sh illumos
