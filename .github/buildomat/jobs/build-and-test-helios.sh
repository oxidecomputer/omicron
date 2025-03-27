#!/bin/bash
#:
#: name = "build-and-test (helios)"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = [
#:	"%/work/*",
#:	"%/work/oxidecomputer/omicron/target/nextest/ci/junit.xml",
#:	"%/var/tmp/omicron_tmp/**/*",
#:	"!/var/tmp/omicron_tmp/crdb-base*",
#:	"!/var/tmp/omicron_tmp/rustc*",
#: ]
#: access_repos = [
#:	"oxidecomputer/dendrite-os"
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

exec .github/buildomat/build-and-test.sh illumos
