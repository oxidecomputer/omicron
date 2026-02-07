#!/bin/bash
#:
#: name = "build-and-test (helios)"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = [
#:	"%/work/*",
#:	"%/work/oxidecomputer/omicron/target/nextest/ci/junit.xml",
#:	"%/work/oxidecomputer/omicron/target/live-tests-archive.tgz",
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
#: name = "crate-build-timings.json"
#: from_output = "/work/crate-build-timings.json"
#:
#: [[publish]]
#: series = "build-info-without-nexus-helios"
#: name = "cargo-timing-without-nexus.html"
#: from_output = "/work/cargo-timing-without-nexus.html"
#:
#: [[publish]]
#: series = "build-info-with-nexus-helios"
#: name = "cargo-timing-with-nexus.html"
#: from_output = "/work/cargo-timing-with-nexus.html"
#:
#: [[publish]]
#: series = "live-tests"
#: name = "live-tests-archive.tgz"
#: from_output = "/work/oxidecomputer/omicron/target/live-tests-archive.tgz"

exec .github/buildomat/build-and-test.sh illumos
