#!/bin/bash
#:
#: name = "build-and-test (ubuntu-22.04)"
#: variety = "basic"
#: target = "ubuntu-22.04"
#: rust_toolchain = true
#: output_rules = [
#:	"%/work/*",
#:	"%/work/oxidecomputer/omicron/target/nextest/ci/junit.xml",
#:	"%/var/tmp/omicron_tmp/**/*",
#:	"!/var/tmp/omicron_tmp/crdb-base*",
#:	"!/var/tmp/omicron_tmp/rustc*",
#: ]
#: access_repos = [
#:	"oxidecomputer/dendrite",
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

sudo apt-get install -y jq
exec .github/buildomat/build-and-test.sh linux
