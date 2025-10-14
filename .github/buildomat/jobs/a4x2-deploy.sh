#!/bin/bash
#:
#: name = "a4x2-deploy"
#: variety = "basic"
#: target = "lab-2.0-gimlet-opte-0.36"
#: output_rules = [
#:	"/out/logs/**/*",
#:  "/out/connectivity-report.json",
#: ]
#: skip_clone = true
#: enable = true
#:
#: [dependencies.a4x2]
#: job = "a4x2-prepare"

set -o errexit
set -o pipefail
set -o xtrace

# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

pfexec mkdir -p /out
pfexec mkdir -p /out/logs
pfexec chown -R "$UID" /out

cp /input/a4x2/xtask .
chmod +x xtask

capture_dianostics() {
    df -h

    # show what services have issues
    for gimlet in g0 g1 g2 g3; do
        ./a4x2 exec $gimlet "svcs -xvZ"
    done

    ./xtask a4x2 deploy collect-evidence
    mv target/a4x2/deploy/output-logs/* /out/logs/

    cp connectivity-report.json /out/
}

#
# If we fail, try to collect some debugging information
#
_exit_trap() {
	local status=$?
	[[ $status -eq 0 ]] && exit 0

    set +o errexit

    capture_dianostics
}
trap _exit_trap EXIT

#
# Make space for CI work
#

# informational diskinfo for the logs
pfexec diskinfo

# Grab one of the U.2 drives by their Product ID, and format it for work
DISK="$(pfexec diskinfo | awk '$4 ~ /WUS4/ { print $2 }' | head -n1)"
pfexec zpool create -o ashift=12 -f cpool "$DISK"
pfexec zfs create -o mountpoint=/ci cpool/ci

#
# Define a zpool for falcon images and disks. Falcon will create the dataset
# on demand.
#
export FALCON_DATASET=cpool/falcon

pfexec chown "$UID" /ci
cd /ci

#
# Fetch the a4x2 topology manager program
#
# buildomat_url=https://buildomat.eng.oxide.computer
# testbed_artifact_path=public/file/oxidecomputer/testbed/topo/
# testbed_rev=67454d38958bcf51830850aec36600df84b7d8a0
# curl -fOL $buildomat_url/$testbed_artifact_path/$testbed_rev/a4x2
# chmod +x a4x2



#
# Run the VM dhcp server
#
#export EXT_INTERFACE=${EXT_INTERFACE:-igb0}

# cp /input/a4x2/out/dhcp-server .
# chmod +x dhcp-server
# first=`bmat address ls -f extra -Ho first`
# last=`bmat address ls -f extra -Ho last`
# gw=`bmat address ls -f extra -Ho gateway`
# server=`ipadm show-addr $EXT_INTERFACE/dhcp -po ADDR | sed 's#/.*##g'`
# pfexec ./dhcp-server $first $last $gw $server &> /out/dhcp-server.log &

#
# Run the topology
# XXX set env var to pull from http://catacomb.eng.oxide.computer:12346/falcon/
#
./xtask a4x2 deploy start --package /input/a4x2/a4x2-package.tar.gz

pfexec dladm
pfexec ipadm
pfexec netstat -nr

#
# Run the communications test program
#
# TODO tighten up packet loss tolerance. For now it's more or less ok for it to
# just run with _some_ comms. The program will fail if there are no comms to a
# given sled.
#
# XXX leave here? move to a4x2_deploy? dunno, stubbed out until we confirm
# CI runs at all on the gimlet
# cp /input/a4x2/out/commtest .
# chmod +x commtest
# NO_COLOR=1 pfexec ./commtest \
#     --api-timeout 30m \
#     http://198.51.100.23 run \
#     --ip-pool-begin 198.51.100.40 \
#     --ip-pool-end 198.51.100.70 \
#     --icmp-loss-tolerance 500 \
#     --test-duration 200s \
#     --packet-rate 10

./xtask a4x2 deploy run-live-tests

collect_evidence
