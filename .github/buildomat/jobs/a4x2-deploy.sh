#!/bin/bash
#:
#: name = "a4x2-deploy"
#: variety = "basic"
#: target = "lab-2.0-opte-0.27"
#: output_rules = [
#:	"/out/falcon/*.log",
#:	"/out/falcon/*.err",
#:  "/out/connectivity-report.json",
#:  "/ci/out/*-sled-agent.log",
#:  "/ci/out/*cockroach*.log",
#:  "%/out/dhcp-server.log",
#: ]
#: skip_clone = true
#: enable = false
#:
#: [dependencies.a4x2]
#: job = "a4x2-prepare"

set -o errexit
set -o pipefail
set -o xtrace

pfexec mkdir -p /out
pfexec chown "$UID" /out

#
# If we fail, try to collect some debugging information
#
_exit_trap() {
	local status=$?
	[[ $status -eq 0 ]] && exit 0

    set +o errexit

    df -h

    # show what services have issues
    for gimlet in g0 g1 g2 g3; do
        ./a4x2 exec $gimlet "svcs -xvZ"
    done

    mkdir -p /out/falcon
    cp .falcon/* /out/falcon/
    for x in ce cr1 cr2 g0 g1 g2 g3; do
        mv /out/falcon/$x.out /out/falcon/$x.log
    done
    cp connectivity-report.json /out/

    mkdir -p /ci/out

    for gimlet in g0 g1 g2 g3; do
        ./a4x2 exec                                           \
            $gimlet                                           \
            "cat /var/svc/log/oxide-sled-agent:default.log" > \
            /ci/out/$gimlet-sled-agent.log
    done

    # collect cockroachdb logs
    mkdir -p /ci/log
    for gimlet in g0 g1 g2 g3; do
        ./a4x2 exec $gimlet 'cat /pool/ext/*/crypt/zone/oxz_cockroachdb*/root/data/logs/cockroach.log' > \
            /ci/out/$gimlet-cockroach.log

        ./a4x2 exec $gimlet 'cat /pool/ext/*/crypt/zone/oxz_cockroachdb*/root/data/logs/cockroach-stderr.log' > \
            /ci/out/$gimlet-cockroach-stderr.log

        ./a4x2 exec $gimlet 'cat /pool/ext/*/crypt/zone/oxz_cockroachdb*/root/data/logs/cockroach-health.log' > \
            /ci/out/$gimlet-cockroach-health.log

        ./a4x2 exec $gimlet 'cat /pool/ext/*/crypt/zone/oxz_cockroachdb*/root/var/svc/log/oxide-cockroachdb:default.log*' > \
            /ci/out/$gimlet-oxide-cockroachdb.log
    done
}
trap _exit_trap EXIT

#
# Install propolis
#
curl -fOL https://buildomat.eng.oxide.computer/wg/0/artefact/01HJ4BJJY2Q9EKXHYV6HQZ8XPN/qQS2fnkS9LebcL4cDLeHRWdleSiXaGKEXGLDucRoab8pwBSi/01HJ4BJY5F995ET252YSD4NJWV/01HJ4CGFH946THBF0ZRH6SRM8X/propolis-server
chmod +x propolis-server
pfexec mv propolis-server /usr/bin/

#
# Make space for CI work
#
export DISK=${DISK:-c1t1d0}
pfexec diskinfo
pfexec zpool create -o ashift=12 -f cpool $DISK
pfexec zfs create -o mountpoint=/ci cpool/ci

if [[ $(curl -s http://catacomb.eng.oxide.computer:12346/trim-me) =~ "true" ]]; then
    pfexec zpool trim cpool
    while [[ ! $(zpool status -t cpool) =~ "100%" ]]; do sleep 10; done
fi

pfexec chown "$UID" /ci
cd /ci

#
# Fetch and decompress the cargo bay from the a4x2-prepeare job
#
for x in ce cr1 cr2 omicron-common g0 g1 g2 g3 tools; do
    tar -xvzf /input/a4x2/out/cargo-bay-$x.tgz
done

for sled in g0 g1 g2 g3; do
    cp -r cargo-bay/omicron-common/omicron/out/* cargo-bay/$sled/omicron/out/
done
ls -R

#
# Fetch the a4x2 topology manager program
#
buildomat_url=https://buildomat.eng.oxide.computer
testbed_artifact_path=public/file/oxidecomputer/testbed/topo/
testbed_rev=67454d38958bcf51830850aec36600df84b7d8a0
curl -fOL $buildomat_url/$testbed_artifact_path/$testbed_rev/a4x2
chmod +x a4x2

#
# Create a zpool for falcon images and disks
#

#
# Install falcon base images
#
export FALCON_DATASET=cpool/falcon
images="debian-11.0_0 helios-2.0_0"
for img in $images; do
    file=$img.raw.xz
    curl -OL http://catacomb.eng.oxide.computer:12346/falcon/$file
    unxz --keep -T 0 $file

    file=$img.raw
    name=${img%_*}
    fsize=`ls -l $img.raw | awk '{print $5}'`
    let vsize=(fsize + 4096 - size%4096)

    pfexec zfs create -p -V $vsize -o volblocksize=4k "$FALCON_DATASET/img/$name"
    pfexec dd if=$img.raw of="/dev/zvol/rdsk/$FALCON_DATASET/img/$name" bs=1024k status=progress
    pfexec zfs snapshot "$FALCON_DATASET/img/$name@base"
done

#
# Install OVMF
#
curl -fOL http://catacomb.eng.oxide.computer:12346/falcon/OVMF_CODE.fd
pfexec mkdir -p /var/ovmf
pfexec cp OVMF_CODE.fd /var/ovmf/OVMF_CODE.fd

#
# Fetch the arista image
#
curl -fOL http://catacomb.eng.oxide.computer:12346/falcon/arista.gz.xz
unxz arista.gz.xz
pfexec zfs receive cpool/falcon/img/arista@base < arista.gz

#
# Run the VM dhcp server
#
export EXT_INTERFACE=${EXT_INTERFACE:-igb0}

cp /input/a4x2/out/dhcp-server .
chmod +x dhcp-server
first=`bmat address ls -f extra -Ho first`
last=`bmat address ls -f extra -Ho last`
gw=`bmat address ls -f extra -Ho gateway`
server=`ipadm show-addr $EXT_INTERFACE/dhcp -po ADDR | sed 's#/.*##g'`
pfexec ./dhcp-server $first $last $gw $server &> /out/dhcp-server.log &

#
# Run the topology
#
pfexec ./a4x2 launch

#
# Add a route to the rack ip pool
#

# Get the DHCP address for the external interface of the customer edge VM. This
# VM interface is attached to the host machine's external interface via viona.
customer_edge_addr=$(./a4x2 exec ce \
    "ip -4 -j addr show enp0s10 | jq -r '.[0].addr_info[] | select(.dynamic == true) | .local'")

# Add the route to the rack via the customer edge VM
pfexec dladm
pfexec ipadm
pfexec netstat -nr
pfexec route add 198.51.100.0/24 $customer_edge_addr

#
# Run the communications test program
#
# TODO tighten up packet loss tolerance. For now it's more or less ok for it to
# just run with _some_ comms. The program will fail if there are no comms to a
# given sled.
cp /input/a4x2/out/commtest .
chmod +x commtest
NO_COLOR=1 pfexec ./commtest \
    --api-timeout 30m \
    http://198.51.100.23 run \
    --ip-pool-begin 198.51.100.40 \
    --ip-pool-end 198.51.100.70 \
    --icmp-loss-tolerance 500 \
    --test-duration 200s \
    --packet-rate 10

cp connectivity-report.json /out/
