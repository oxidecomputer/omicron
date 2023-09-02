#!/bin/bash
#:
#: name = "build-and-test (ubuntu-20.04)"
#: variety = "basic"
#: target = "ubuntu-20.04"
#: rust_toolchain = "1.72.0"
#: output_rules = [
#:	"/var/tmp/omicron_tmp/*",
#:	"!/var/tmp/omicron_tmp/crdb-base*",
#:	"!/var/tmp/omicron_tmp/rustc*",
#: ]

exec .github/buildomat/build-and-test.sh linux
