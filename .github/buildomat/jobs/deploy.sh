#!/bin/bash
#:
#: name = "helios / deploy"
#: variety = "basic"
#: target = "lab-opte-0.22"
#: output_rules = [
#:	"%/var/svc/log/oxide-sled-agent:default.log",
#:	"%/zone/oxz_*/root/var/svc/log/oxide-*.log",
#:	"%/zone/oxz_*/root/var/svc/log/system-illumos-*.log",
#: ]
#: skip_clone = true
#:
#: [dependencies.package]
#: job = "helios / package"
#:
#: [dependencies.build-end-to-end-tests]
#: job = "helios / build-end-to-end-tests"

set -o errexit
set -o pipefail
set -o xtrace

#
# If we fail, try to collect some debugging information
#
_exit_trap() {
	local status=$?
	[[ $status -eq 0 ]] && exit 0

	set +o errexit
	set -o xtrace
	banner evidence
	zoneadm list -civ
	pfexec dladm show-phys -m
	pfexec dladm show-link
	pfexec dladm show-vnic
	pfexec ipadm
	pfexec netstat -rncva
	pfexec netstat -anu
	pfexec arp -an
	pfexec ./out/softnpu/scadm \
		--server /opt/oxide/softnpu/stuff/server \
		--client /opt/oxide/softnpu/stuff/client \
		standalone \
		dump-state
	pfexec /opt/oxide/opte/bin/opteadm list-ports
	PORTS=$(pfexec /opt/oxide/opte/bin/opteadm list-ports | tail +2 | awk '{ print $1; }')
	for p in $PORTS; do
		LAYERS=$(pfexec /opt/oxide/opte/bin/opteadm list-layers -p $p | tail +2 | awk '{ print $1; }')
		for l in $LAYERS; do
			pfexec /opt/oxide/opte/bin/opteadm dump-layer -p $p $l
		done
	done

	pfexec zfs list
	pfexec zpool list
	pfexec fmdump -eVp
	pfexec ptree -z global
	pfexec svcs -xv
	for z in $(zoneadm list -n); do
		banner "${z/oxz_/}"
		pfexec svcs -xv -z "$z"
		pfexec ptree -z "$z"
		pfexec zlogin "$z" ipadm
		pfexec zlogin "$z" netstat -rncva
		pfexec zlogin "$z" netstat -anu
		pfexec zlogin "$z" arp -an
	done

	exit $status
}
trap _exit_trap EXIT

#
# XXX work around 14537 (UFS should not allow directories to be unlinked) which
# is probably not yet fixed in xde branch?  Once the xde branch merges from
# master to include that fix, this can go.
#
# NB: The symptom is that std::fs::remove_dir_all() will ruin the ramdisk file
# system in a way that is hard to specifically detect, and some subsequent
# operations will fail in strange ways; e.g., EEXIST on subsequent rmdir(2) of
# an apparently empty directory.
#
pfexec mdb -kwe 'secpolicy_fs_linkdir/v 55 48 89 e5 b8 01 00 00 00 5d c3'

if [[ -d /opt/oxide ]]; then
	#
	# The netdev ramdisk environment contains OPTE, which presently means
	# /opt/oxide already exists as part of the ramdisk.  We want to create
	# a tmpfs at that location so that we can unfurl a raft of extra files,
	# so move whatever is already there out of the way:
	#
	pfexec mv /opt/oxide /opt/oxide-underneath
fi
pfexec mkdir /opt/oxide
pfexec mount -F tmpfs -O swap /opt/oxide
if [[ -d /opt/oxide-underneath ]]; then
	#
	# Copy the original /opt/oxide tree into the new tmpfs:
	#
	(cd /opt/oxide-underneath && pfexec tar ceEp@/f - .) |
	    (cd /opt/oxide && pfexec tar xveEp@/f -)
	pfexec rm -rf /opt/oxide-underneath
fi

#
# XXX the creation of rpool/zone (at "/zone") is perhaps not working as
# expected?  The symptom was that zone creation at /zone would fill up the
# ramdisk, which implies to me that /zone was not properly mounted at the time.
# Explicitly creating it here seems to help.
#
pfexec /sbin/zfs create -o mountpoint=/zone rpool/zone

#
# The sled agent will ostensibly write things into /var/oxide, so make that a
# tmpfs as well:
#
pfexec mkdir -p /var/oxide
pfexec mount -F tmpfs -O swap /var/oxide

pfexec mkdir /opt/oxide/work
pfexec chown build:build /opt/oxide/work
cd /opt/oxide/work

ptime -m tar xvzf /input/package/work/package.tar.gz
cp /input/package/work/zones/* out/
mkdir tests
for p in /input/build-end-to-end-tests/work/*.gz; do
	ptime -m gunzip < "$p" > "tests/$(basename "${p%.gz}")"
	chmod a+x "tests/$(basename "${p%.gz}")"
done

ptime -m pfexec ./tools/create_virtual_hardware.sh

#
# Image-related tests use images served by catacomb. The lab network is
# IPv4-only; the propolis zones are IPv6-only. These steps set up tcpproxy
# configured to proxy to catacomb via port 54321 in the global zone.
#
pfexec mkdir -p /usr/oxide
pfexec rm -f /usr/oxide/tcpproxy
pfexec curl -sSfL -o /usr/oxide/tcpproxy \
	http://catacomb.eng.oxide.computer:12346/tcpproxy
pfexec chmod +x /usr/oxide/tcpproxy
pfexec rm -f /var/svc/manifest/site/tcpproxy.xml
pfexec curl -sSfL -o /var/svc/manifest/site/tcpproxy.xml \
	http://catacomb.eng.oxide.computer:12346/tcpproxy.xml
pfexec svccfg import /var/svc/manifest/site/tcpproxy.xml

#
# This OMICRON_NO_UNINSTALL hack here is so that there is no implicit uninstall
# before the install.  This doesn't work right now because, above, we made
# /var/oxide a file system so you can't remove it (EBUSY) like a regular
# directory.  The lab-netdev target is a ramdisk system that is always cleared
# out between runs, so it has not had any state yet that requires
# uninstallation.
#
OMICRON_NO_UNINSTALL=1 \
    ptime -m pfexec ./target/release/omicron-package -t test install

# NOTE: this command configures proxy arp for softnpu. This is needed if you want to be
# able to reach instances from the same L2 network segment.
# Keep consistent with `get_system_ip_pool` in `end-to-end-tests`.
IP_POOL_START="192.168.1.50"
IP_POOL_END="192.168.1.90"
# `dladm` won't return leading zeroes but `scadm` expects them, use sed to add any missing zeroes
SOFTNPU_MAC=$(dladm show-vnic sc0_1 -p -o macaddress | sed -E 's/[ :]/&0/g; s/0([^:]{2}(:|$))/\1/g')
pfexec ./out/softnpu/scadm \
	--server /opt/oxide/softnpu/stuff/server \
	--client /opt/oxide/softnpu/stuff/client \
	standalone \
	add-proxy-arp $IP_POOL_START $IP_POOL_END $SOFTNPU_MAC

# We also need to configure proxy arp for any services which use OPTE for external connectivity (e.g. Nexus)
tar xf out/omicron-sled-agent.tar pkg/config-rss.toml
SERVICE_IP_POOL_START="$(sed -n 's/first = "\(.*\)"/\1/p' pkg/config-rss.toml)"
SERVICE_IP_POOL_END="$(sed -n 's/last = "\(.*\)"/\1/p' pkg/config-rss.toml)"
rm -r pkg

pfexec ./out/softnpu/scadm \
	--server /opt/oxide/softnpu/stuff/server \
	--client /opt/oxide/softnpu/stuff/client \
	standalone \
	add-proxy-arp $SERVICE_IP_POOL_START $SERVICE_IP_POOL_END $SOFTNPU_MAC

pfexec ./out/softnpu/scadm \
	--server /opt/oxide/softnpu/stuff/server \
	--client /opt/oxide/softnpu/stuff/client \
	standalone \
	dump-state

# Wait for switch zone to come up so that we can configure it
retry=0
until curl --head --silent -o /dev/null "http://[fd00:1122:3344:101::2]:12224/"
do
	if [[ $retry -gt 30 ]]; then
		echo "Failed to reach switch zone after 30 seconds"
		exit 1
	fi
	sleep 1
	retry=$((retry + 1))
done

# Nexus (and any instances using the above IP pool) are configured to use external
# IPs from a fixed subnet (192.168.1.0/24). OPTE/SoftNPU/Boundary Services take care
# of NATing between the private VPC networks and this "external network".
# We create a static IP in this subnet in the global zone and configure the switch
# to use it as the default gateway.
# NOTE: Keep in sync with $[SERVICE_]IP_POOL_{START,END}
export GATEWAY_IP=192.168.1.199
export GATEWAY_MAC=$(dladm show-phys -m -p -o ADDRESS | head -n 1)
pfexec ipadm create-addr -T static -a $GATEWAY_IP/24 igb0/sidehatch

# NOTE: this script configures softnpu's "rack network" settings using swadm
./tools/scrimlet/softnpu-init.sh

export RUST_BACKTRACE=1
./tests/bootstrap

rm ./tests/bootstrap
for test_bin in tests/*; do
	./"$test_bin"
done
