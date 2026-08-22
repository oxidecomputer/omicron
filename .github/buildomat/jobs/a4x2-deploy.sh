#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#:
#: name = "a4x2-deploy"
#: variety = "basic"
#: target = "lab-3.0-opte-0.40"
#: output_rules = [
#:	"/out/falcon/*.log",
#:	"/out/falcon/*.err",
#:  "/out/connectivity-report.json",
#:  "%/out/multicast-connectivity-report.json",
#:  "/ci/out/*-sled-agent.log",
#:  "/ci/out/*cockroach*.log",
#:  "%/ci/out/mcast-omdb-*.log",
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

# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

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
    # The multicast report only exists once the multicast phase has run.
    cp multicast-connectivity-report.json /out/ 2>/dev/null || true

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

    # Capture multicast control-plane state so a failed mcast phase could be
    # deciphered from a data-plane-only failure: a reaped or never-created
    # group, a member stuck out of "Joined", or ddmd peers not in Exchange all
    # look identical from the connectivity report alone. omdb lives in the
    # switch zone on the scrimlets (g0/g3). We try each until one answers.
    for cmd in "db multicast groups" "db multicast members" \
               "db multicast pools" "nexus multicast ddm-peers"; do
        tag=$(echo "$cmd" | tr ' ' '-')
        for scrimlet in g0 g3; do
            if ./a4x2 exec $scrimlet \
                "pfexec zlogin oxz_switch /opt/oxide/omdb/bin/omdb $cmd" \
                > /ci/out/mcast-omdb-$tag.log 2>&1; then
                break
            fi
        done
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
images="debian-11.0_0 helios-3.0_0"
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

# commtest sends multicast from the host. cr1 also sits on the host-facing L2
# segment, so we mirror those test frames directly from cr1's host-facing NIC
# toward the switch-facing NICs instead of running a multicast routing daemon in
# the VM images themselves. The group's elected switch is picked later from its
# Nexus-assigned UUID, which means this setup cannot know which sidecar owns the
# external NAT entry. Mirror each group to both switch-facing sidecars and let
# the non-elected switch drop its copy, since it has no matching external entry.
mcast_groups=("239.100.0.1" "239.100.0.2")

# We resolve the host-facing interface via a route lookup toward the multicast
# source (the host's external address commtest sends from). This avoids
# hardcoding names that vary with a4x2 NIC enumeration. The host-facing
# NIC sits in the router's management VRF, which hides the connected /24 from
# the main table, so the lookup must be VRF-scoped. An unscoped lookup follows
# the BGP default out a rack-facing NIC instead.
mcast_inbound_iface() {
    local node=$1 fallback=$2 iface
    # Tolerate a non-zero pipeline (route lookup failure, or head closing the
    # pipe early) so errexit does not abort before the fallback applies.
    iface=$(./a4x2 exec "$node" "ip -o route get $server vrf mgmt" 2>/dev/null \
        | tr -d '\r' | sed -n 's/.* dev \([^ ]\{1,\}\).*/\1/p' | head -1) || true
    # Guard against an empty or rack-facing result (no mgmt VRF on older
    # images, or a lookup that resolved via BGP) by falling back to the
    # host-facing default.
    case "$iface" in
        ""|enp0s8|enp0s9|enp0s10) iface=$fallback ;;
    esac
    printf '%s' "$iface"
}

# Mirror each external multicast group to every switch-facing sidecar.
#
# This mirror is the inbound external path only, as commtest sources multicast
# from the host, which must ingress at the switch that owns the group's external
# NAT entry. Designated-forwarder election in Nexus places that entry on a
# single switch chosen by a group-UUID hash. The UUID is allocated later and is
# unknown here, so the script cannot predict which sidecar is the elected one.
# Mirroring to both sidecars is a robust solution: only the elected switch holds
# the external NAT entry and replicates to the underlay. The other switch has no
# matching entry and drops its copy, so nothing is replicated twice. flower is
# tc's flow-field packet classifier, matching each group
# by dst_ip.
mcast_mirror() {
    local node=$1 iif=$2; shift 2
    local oifs=("$@") g out pref=100 actions

    # Ensure the shared clsact qdisc exists without recreating it. Deleting
    # it would drop every ingress filter on the device, not just the mirror
    # set this function owns. Per-group filters below use `replace` with an
    # explicit handle, which is idempotent across reruns.
    ./a4x2 exec "$node" "tc qdisc add dev $iif clsact 2>/dev/null || true"
    for g in "${mcast_groups[@]}"; do
        # All sidecars must be mirred actions chained in one filter. Separate
        # per-sidecar filters do not work because the first matching filter ends
        # flower classification for the packet, so later filters never fire
        # and groups elected to the second switch go undelivered. Chained
        # actions all execute, since mirred's default control is pipe.
        actions=""
        for out in "${oifs[@]}"; do
            actions+=" action mirred egress mirror dev $out"
        done
        echo "  $node mirror $g: $iif -> ${oifs[*]}"
        # The explicit handle keeps `replace` idempotent. Left at 0, the
        # kernel treats a rerun as a fresh insert and flower returns EEXIST
        # for the duplicate key.
        ./a4x2 exec "$node" \
            "tc filter replace dev $iif ingress handle 1 pref $pref protocol ip \
             flower dst_ip $g$actions"
        pref=$((pref + 1))
    done
    ./a4x2 exec "$node" "tc filter show dev $iif ingress"
}

# commtest never sets IP_MULTICAST_IF, so the egress interface for each group
# comes from the routing table. Point every group at the customer edge, the
# same treatment the pool route above gives unicast, or the frames leave the
# host on its default multicast interface and never reach the a4x2 segment.
for g in "${mcast_groups[@]}"; do
    # Delete first so a rerun on a warm host does not trip errexit on an
    # already-present route.
    pfexec route delete -host "$g" 2>/dev/null || true
    pfexec route add -host "$g" "$customer_edge_addr"
done

cr1_iif=$(mcast_inbound_iface cr1 enp0s11)
echo "mcast mirror inbound: cr1=$cr1_iif"
mcast_mirror cr1 "$cr1_iif" enp0s9 enp0s10

#
# Plumb host-sourced multicast into the rack
#

# commtest sends multicast from the host. cr1 also sits on the host-facing L2
# segment, so mirror those test frames directly from cr1's host-facing NIC
# toward the switch-facing NICs instead of running a multicast routing daemon
# in the VM images. The group's elected switch is picked later from its
# Nexus-assigned UUID, so this setup cannot know which sidecar owns the
# external NAT entry. Mirror each group to both switch-facing sidecars and let
# the non-elected switch drop its copy for want of a matching external entry.
mcast_groups=("239.100.0.1")

# We resolve the host-facing interface by asking which link routes back to the
# multicast source (the host's external address commtest sends from). This
# avoids hardcoding names that vary with a4x2 NIC enumeration. The host-facing
# NIC sits in the router's management VRF, which hides the connected /24 from
# the main table, so the lookup must be VRF-scoped. An unscoped lookup follows
# the BGP default out a rack-facing NIC instead.
mcast_inbound_iface() {
    local node=$1 fallback=$2 iface
    # Tolerate a non-zero pipeline (route lookup failure, or head closing the
    # pipe early) so errexit does not abort before the fallback applies.
    iface=$(./a4x2 exec "$node" "ip -o route get $server vrf mgmt" 2>/dev/null \
        | tr -d '\r' | sed -n 's/.* dev \([^ ]\{1,\}\).*/\1/p' | head -1) || true
    # Guard against an empty or rack-facing result (no mgmt VRF on older
    # images, or a lookup that resolved via BGP) by falling back to the
    # host-facing default.
    case "$iface" in
        ""|enp0s8|enp0s9|enp0s10) iface=$fallback ;;
    esac
    printf '%s' "$iface"
}

# The Ethernet address a group's frames carry on the wire: the group's
# low-order 23 bits placed into 01:00:5e:00:00:00 (RFC 1112 section 6.4).
mcast_group_mac() {
    local g=$1 a b c d
    IFS=. read -r a b c d <<< "$g"
    printf '01:00:5e:%02x:%02x:%02x' $((b & 0x7f)) "$c" "$d"
}

# Mirror each external multicast group to every switch-facing sidecar.
#
# This mirror is the inbound external path only, as commtest sources multicast
# from the host, which must ingress at the switch that owns the group's
# external NAT entry. Designated-forwarder election in Nexus places that entry
# on a single switch chosen by a group-UUID hash. The UUID is allocated later
# and is unknown here, so the script cannot predict which sidecar is the
# elected one. Mirroring to both sidecars is a robust solution: only the
# elected switch holds the external NAT entry and replicates to the underlay,
# while the other ingests the copy and drops it for want of a matching entry,
# so there is no duplicate replication. flower is tc's flow-field packet
# classifier, matching each group by dst_ip.
mcast_mirror() {
    local node=$1 iif=$2; shift 2
    local oifs=("$@") g out mac pref=100 actions

    # Recreate clsact rather than replacing it in place, so filters left by a
    # previous install (possibly at other prefs or with other actions) cannot
    # linger alongside the new set.
    ./a4x2 exec "$node" "tc qdisc del dev $iif clsact 2>/dev/null || true"
    ./a4x2 exec "$node" "tc qdisc add dev $iif clsact"
    for g in "${mcast_groups[@]}"; do
        # All sidecars must be mirred actions chained in one filter. Separate
        # per-sidecar filters do not work because the first matching filter
        # ends flower classification for the packet, so later filters never
        # fire and groups elected to the second switch go undelivered. Chained
        # actions all execute, since mirred's default control is pipe.
        actions=""
        for out in "${oifs[@]}"; do
            actions+=" action mirred egress mirror dev $out"
        done
        echo "  $node mirror $g: $iif -> ${oifs[*]}"
        ./a4x2 exec "$node" \
            "tc filter replace dev $iif ingress pref $pref protocol ip \
             flower dst_ip $g$actions"
        pref=$((pref + 1))

        # The mirror only sees frames the NIC accepts, and a NIC accepts a
        # multicast group only after a join. Nothing in the router joins these
        # groups (FRR speaks no multicast protocol), so a static link-layer
        # membership for the group's Ethernet address stands in for the join.
        # The membership dies with the VM, so a one-shot CI run needs no
        # teardown.
        mac=$(mcast_group_mac "$g")
        ./a4x2 exec "$node" "ip maddress add $mac dev $iif"
    done
    ./a4x2 exec "$node" "tc filter show dev $iif ingress"
}

# commtest never sets IP_MULTICAST_IF, so the egress interface for each group
# comes from the routing table. Point every group at the customer edge, the
# same treatment the pool route above gives unicast, or the frames leave the
# host on its default multicast interface and never reach the a4x2 segment.
for g in "${mcast_groups[@]}"; do
    # Delete first so a rerun on a warm host does not trip errexit on an
    # already-present route.
    pfexec route delete -host "$g" 2>/dev/null || true
    pfexec route add -host "$g" "$customer_edge_addr"
done

cr1_iif=$(mcast_inbound_iface cr1 enp0s11)
echo "mcast mirror inbound: cr1=$cr1_iif"
mcast_mirror cr1 "$cr1_iif" enp0s9 enp0s10

#
# Run the communications test program
#
# TODO tighten up packet loss tolerance. For now it's more or less ok for it to
# just run with _some_ comms. The program will fail if there are no comms to a
# given sled.
cp /input/a4x2/out/commtest .
chmod +x commtest
mcast_args=()
for g in "${mcast_groups[@]}"; do
    mcast_args+=(--mcast-group "$g")
done
NO_COLOR=1 pfexec ./commtest \
    --api-timeout 30m \
    http://198.51.100.23 run \
    --ip-pool-begin 198.51.100.40 \
    --ip-pool-end 198.51.100.70 \
    --icmp-loss-tolerance 500 \
    --warmup 30s \
    --test-duration 200s \
    --packet-rate 10 \
    "${mcast_args[@]}"

cp connectivity-report.json /out/
cp multicast-connectivity-report.json /out/
