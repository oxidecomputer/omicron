#!/bin/bash
#:
#: name = "helios / deploy"
#: variety = "basic"
#: target = "lab"
#: output_rules = [
#:	"/var/oxide/sled-agent.log",
#: ]
#: skip_clone = true
#:
#: [dependencies.package]
#: job = "helios / package"
#:

set -o errexit
set -o pipefail
set -o xtrace

pfexec mkdir /opt/oxide
pfexec mount -F tmpfs -O swap /opt/oxide
pfexec mkdir /opt/oxide/work
pfexec chown build:build /opt/oxide/work
cd /opt/oxide/work

ptime -m tar xvzf /input/package/work/package.tar.gz
ptime -m pfexec ./tools/create_virtual_hardware.sh
ptime -m pfexec ./target/release/omicron-package install

# Wait up to 5 minutes for RSS to say it's done
for _i in {1..30}; do
	grep 'Finished setting up services' /var/oxide/sled-agent.log && break
	sleep 10
done

# TODO: write tests and run the resulting test bin here
curl -i http://[fd00:1122:3344:0101::3]:12220

ptime -m pfexec ./target/release/omicron-package uninstall
ptime -m pfexec ./tools/destroy_virtual_hardware.sh
