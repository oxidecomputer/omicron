#!/bin/bash
#:
#: name = "helios / deploy"
#: variety = "basic"
#: target = "lab-2.0-opte-0.36"
#: output_rules = [
#:  "%/var/svc/log/oxide-*.log*",
#:  "%/zone/oxz_*/root/var/svc/log/oxide-*.log*",
#:  "%/pool/ext/*/crypt/zone/oxz_*/root/var/svc/log/oxide-*.log*",
#:  "%/pool/ext/*/crypt/zone/oxz_*/root/var/svc/log/system-illumos-*.log*",
#:  "%/pool/ext/*/crypt/zone/oxz_ntp_*/root/var/log/chrony/*.log*",
#:  "!/pool/ext/*/crypt/zone/oxz_propolis-server_*/root/var/svc/log/*.log*",
#:  "%/pool/ext/*/crypt/debug/global/oxide-sled-agent:default.log.*",
#:  "%/pool/ext/*/crypt/debug/oxz_*/oxide-*.log.*",
#:  "%/pool/ext/*/crypt/debug/oxz_*/system-illumos-*.log.*",
#:  "!/pool/ext/*/crypt/debug/oxz_propolis-server_*/*.log.*",
#:  "/tmp/kstat/*.kstat"
#: ]
#: skip_clone = true
#:
#: [dependencies.package]
#: job = "helios / package"
#:

set -o errexit
set -o pipefail
set -o xtrace

#
# If we fail, try to collect some debugging information
#
_exit_trap() {
	local status=$?
	set +o errexit

	if [[ "x$OPTE_COMMIT" != "x" ]]; then
		pfexec cp /tmp/opteadm /opt/oxide/opte/bin/opteadm
	fi

	#
	# Stop cron in all zones (to stop logadm log rotation)
	#
	pfexec svcadm -Z disable -s cron

	[[ $status -eq 0 ]] && exit 0

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
	pfexec zlogin sidecar_softnpu /softnpu/scadm \
		--server /softnpu/server \
		--client /softnpu/client \
		standalone \
		dump-state
	pfexec /opt/oxide/opte/bin/opteadm list-ports
	pfexec /opt/oxide/opte/bin/opteadm dump-v2b
	pfexec /opt/oxide/opte/bin/opteadm dump-v2p
	z_swadm link ls
	z_swadm addr list
	z_swadm route list
	z_swadm arp list
	z_swadm nat list

	PORTS=$(pfexec /opt/oxide/opte/bin/opteadm list-ports | tail +2 | awk '{ print $1; }')
	for p in $PORTS; do
		pfexec /opt/oxide/opte/bin/opteadm dump-uft -p $p
		LAYERS=$(pfexec /opt/oxide/opte/bin/opteadm list-layers -p $p | tail +2 | awk '{ print $1; }')
		for l in $LAYERS; do
			pfexec /opt/oxide/opte/bin/opteadm dump-layer -p $p $l
		done
	done

	mkdir -p /tmp/kstat
	pfexec kstat -p xde: > /tmp/kstat/xde.kstat

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

	for z in $(zoneadm list -n | grep oxz_ntp); do
		banner "${z/oxz_/}"
		pfexec zlogin "$z" chronyc -n tracking
		pfexec zlogin "$z" chronyc -n sources -a
		pfexec zlogin "$z" cat /etc/inet/chrony.conf
		pfexec zlogin "$z" ping -sn oxide.computer 56 1
		pfexec zlogin "$z" ping -sn 1.1.1.1 56 1
		pfexec zlogin "$z" /usr/sbin/dig 0.pool.ntp.org @1.1.1.1
		pfexec zlogin "$z" getent hosts time.cloudfare.com

		# Attempt to get chrony to do some time sync from the CLI with
		# messages being written to the terminal and with debugging
		# enabled if the chrony package was built with that option.
		# Since chronyd on the CLI needs to use the ports that the
		# service will be using, stop it first (with -s to wait for it
		# to exit).
		pfexec /usr/sbin/svcadm -z "$z" disable -s oxide/ntp
		# Run in dry-run one-shot mode (-Q)
		pfexec zlogin "$z" /usr/sbin/chronyd -t 10 -ddQ
		# Run in one-shot mode (-q) -- attempt to set the clock
		pfexec zlogin "$z" /usr/sbin/chronyd -t 10 -ddq
		# Run in one-shot mode (-q) but override the configuration
		# to talk to an explicit external service. This command line is
		# similar to that used by the pre-flight NTP checks.
		pfexec zlogin "$z" /usr/sbin/chronyd -t 10 -ddq \
		    "'pool time.cloudflare.com iburst maxdelay 0.1'"
	done

	pfexec zlogin sidecar_softnpu cat /var/log/softnpu.log

	exit $status
}
trap _exit_trap EXIT

z_swadm () {
	echo "== swadm $@"
	pfexec zlogin oxz_switch /opt/oxide/dendrite/bin/swadm $@
}

# only set this if you want to override the version of opte/xde installed by the
# install_opte.sh script
OPTE_COMMIT="792ec8a3816ba8c7c4268f65cd19dbd946a9027d"
if [[ "x$OPTE_COMMIT" != "x" ]]; then
	curl  -sSfOL https://buildomat.eng.oxide.computer/public/file/oxidecomputer/opte/module/$OPTE_COMMIT/xde
	pfexec rem_drv xde || true
	pfexec mv xde /kernel/drv/amd64/xde
	pfexec add_drv xde || true
	curl  -sSfOL https://buildomat.eng.oxide.computer/public/file/oxidecomputer/opte/release/$OPTE_COMMIT/opteadm
	chmod +x opteadm
	cp opteadm /tmp/opteadm
	pfexec mv opteadm /opt/oxide/opte/bin/opteadm
fi

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

pfexec mkdir /opt/oxide/work
pfexec chown build:build /opt/oxide/work
cd /opt/oxide/work

ptime -m tar xvzf /input/package/work/package.tar.gz

# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

# Ask buildomat for the range of extra addresses that we're allowed to use, and
# break them up into the ranges we need.

bmat address ls
set -- $(bmat address ls -f extra -Ho first,last,count)
EXTRA_IP_START="${1:?No extra start IP address found}"
EXTRA_IP_END="${2:?No extra end IP address found}"
EXTRA_IP_COUNT="${3:?No extra IP address count found}"

# We need at least 32 IP addresses
((EXTRA_IP_COUNT >= 32))

EXTRA_IP_BASE="${EXTRA_IP_START%.*}"
EXTRA_IP_FOCTET="${EXTRA_IP_START##*.}"
EXTRA_IP_LOCTET="${EXTRA_IP_END##*.}"

# We will break up this additional IP address range as follows (offsets from
# base address shown)
#
# 0-9	internal service pool
#     0 external DNS server
#     1 external DNS server
#
# 10	infra IP/uplink
# 11+   IP pool

SERVICE_IP_POOL_START="$EXTRA_IP_BASE.$((EXTRA_IP_FOCTET + 0))"
SERVICE_IP_POOL_END="$EXTRA_IP_BASE.$((EXTRA_IP_FOCTET + 9))"
DNS_IP1="$EXTRA_IP_BASE.$((EXTRA_IP_FOCTET + 0))"
DNS_IP2="$EXTRA_IP_BASE.$((EXTRA_IP_FOCTET + 1))"
UPLINK_IP="$EXTRA_IP_BASE.$((EXTRA_IP_FOCTET + 10))"
IPPOOL_START="$EXTRA_IP_BASE.$((EXTRA_IP_FOCTET + 11))"
IPPOOL_END="$EXTRA_IP_BASE.$((EXTRA_IP_LOCTET + 0))"

# Set the gateway IP address to be the GZ IP...
GATEWAY_IP=$(ipadm show-addr -po type,addr | \
    awk -F'[:/]' '$1 == "dhcp" {print $2}')
[[ -n "$GATEWAY_IP" ]]
ping -s "$GATEWAY_IP" 56 1 || true
GATEWAY_MAC=$(arp -an | awk -vgw=$GATEWAY_IP '$2 == gw {print $NF}')
# ...and enable IP forwarding so that zones can reach external networks
# through the GZ. This allows the NTP zone to talk to DNS and NTP servers on
# the Internet.
routeadm -e ipv4-forwarding -u

# Configure softnpu to proxy ARP for the entire extra IP range.
PXA_START="$EXTRA_IP_START"
PXA_END="$EXTRA_IP_END"

pfexec zpool create -f scratch c1t1d0 c2t1d0

ptime -m \
    pfexec ./target/release/xtask virtual-hardware \
    --vdev-dir /scratch \
    create \
    --gateway-ip "$GATEWAY_IP" \
    --gateway-mac "$GATEWAY_MAC" \
    --pxa-start "$PXA_START" \
    --pxa-end "$PXA_END"

#
# Generate a self-signed certificate to use as the initial TLS certificate for
# the recovery Silo.  Its DNS name is determined by the silo name and the
# delegated external DNS name, both of which are in the RSS config file.  In a
# real system, the certificate would come from the customer during initial rack
# setup on the technician port.
#
tar xf out/omicron-sled-agent.tar pkg/config-rss.toml pkg/config.toml

# Update the vdevs to point to where we've created them
sed -E -i~ "s/(m2|u2)(.*\.vdev)/\/scratch\/\1\2/g" pkg/config.toml
diff -u pkg/config.toml{~,} || true

EXPECTED_ZPOOL_COUNT=$(grep -c -E 'u2.*\.vdev' pkg/config.toml)
echo "expected number of zpools is ${EXPECTED_ZPOOL_COUNT}"

SILO_NAME="$(sed -n 's/silo_name = "\(.*\)"/\1/p' pkg/config-rss.toml)"
EXTERNAL_DNS_DOMAIN="$(sed -n 's/external_dns_zone_name = "\(.*\)"/\1/p' pkg/config-rss.toml)"

# Substitute addresses from the external network range into the RSS config.
sed -i~ "
	/^external_dns_ips/c\\
external_dns_ips = [ \"$DNS_IP1\", \"$DNS_IP2\" ]
	/^\\[\\[internal_services_ip_pool_ranges/,/^\$/ {
		/^first/c\\
first = \"$SERVICE_IP_POOL_START\"
		/^last/c\\
last = \"$SERVICE_IP_POOL_END\"
	}
	/^infra_ip_first/c\\
infra_ip_first = \"$UPLINK_IP\"
	/^infra_ip_last/c\\
infra_ip_last = \"$UPLINK_IP\"
	/^\\[\\[rack_network_config.ports/,/^\$/ {
		/^routes/c\\
routes = \\[{nexthop = \"$GATEWAY_IP\", destination = \"0.0.0.0/0\"}\\]
		/^addresses/c\\
addresses = \\[{address = \"$UPLINK_IP/24\"} \\]
	}
" pkg/config-rss.toml
diff -u pkg/config-rss.toml{~,} || true

tar rvf out/omicron-sled-agent.tar pkg/config-rss.toml pkg/config.toml
rm -f pkg/config-rss.toml* pkg/config.toml*

#
# By default, OpenSSL creates self-signed certificates with "CA:true".  The TLS
# implementation used by reqwest rejects endpoint certificates that are also CA
# certificates.  So in order to use the certificate, we need one without
# "CA:true".  There doesn't seem to be a way to do this on the command line.
# Instead, we must override the system configuration with our own configuration
# file.  There's virtually nothing in it.
#
TLS_NAME="$SILO_NAME.sys.$EXTERNAL_DNS_DOMAIN"
openssl req \
    -newkey rsa:4096 \
    -x509 \
    -sha256 \
    -days 3 \
    -nodes \
    -out "pkg/initial-tls-cert.pem" \
    -keyout "pkg/initial-tls-key.pem" \
    -subj "/CN=$TLS_NAME" \
    -addext "subjectAltName=DNS:$TLS_NAME" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -config /dev/stdin <<EOF
[req]
prompt = no
distinguished_name = req_distinguished_name

[req_distinguished_name]
EOF
tar rvf out/omicron-sled-agent.tar \
    pkg/initial-tls-cert.pem \
    pkg/initial-tls-key.pem
rm -f pkg/initial-tls-cert.pem pkg/initial-tls-key.pem
rmdir pkg
# The actual end-to-end tests need the certificate.  This is where that file
# will end up once installed.
E2E_TLS_CERT="/opt/oxide/sled-agent/pkg/initial-tls-cert.pem"

#
# Download the Oxide CLI and images from catacomb.
#
pfexec mkdir -p /usr/oxide
pfexec curl -sSfL -o /usr/oxide/oxide \
	http://catacomb.eng.oxide.computer:12346/oxide-v0.1.1
pfexec chmod +x /usr/oxide/oxide

curl -sSfL -o debian-11-genericcloud-amd64.raw \
	http://catacomb.eng.oxide.computer:12346/debian-11-genericcloud-amd64.raw

#
# The lab-netdev target is a ramdisk system that is always cleared
# out between runs, so it has not had any state yet that requires
# uninstallation.
#
OMICRON_NO_UNINSTALL=1 \
    ptime -m pfexec ./target/release/omicron-package -t test install

# Wait for switch zone to come up
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

pfexec zlogin sidecar_softnpu /softnpu/scadm \
	--server /softnpu/server \
	--client /softnpu/client \
	standalone \
	dump-state

# Wait for the chrony service in the NTP zone to come up
retry=0
while [[ $(pfexec svcs -z $(zoneadm list -n | grep oxz_ntp) \
    -Hostate oxide/ntp || true) != online ]]; do
	if [[ $retry -gt 60 ]]; then
		echo "NTP zone chrony failed to come up after 60 seconds"
		exit 1
	fi
	sleep 1
	retry=$((retry + 1))
done
echo "Waited for chrony: ${retry}s"

# Wait for at least one nexus zone to become available
retry=0
until zoneadm list | grep nexus; do
	if [[ $retry -gt 300 ]]; then
		echo "Failed to start at least one nexus zone after 300 seconds"
		exit 1
	fi
	sleep 1
	retry=$((retry + 1))
done
echo "Waited for nexus: ${retry}s"

# Wait for handoff, as zpools as inserted into the database during
# `rack_initialize`, and the next omdb command requires them to exist in the
# db.
retry=0
until grep "Handoff to Nexus is complete" /var/svc/log/oxide-sled-agent:default.log; do
	if [[ $retry -gt 300 ]]; then
		echo "Failed to handoff to Nexus after 300 seconds"
		exit 1
	fi
	sleep 1
	retry=$((retry + 1))
done
echo "Waited for handoff: ${retry}s"

# Wait for the number of expected U2 zpools
retry=0
ACTUAL_ZPOOL_COUNT=$(pfexec zlogin oxz_switch /opt/oxide/omdb/bin/omdb db zpool list -i | wc -l)
until [[ "${ACTUAL_ZPOOL_COUNT}" -eq "${EXPECTED_ZPOOL_COUNT}" ]];
do
	pfexec zlogin oxz_switch /opt/oxide/omdb/bin/omdb db zpool list
	if [[ $retry -gt 300 ]]; then
		echo "Failed to wait for ${EXPECTED_ZPOOL_COUNT} zpools after 300 seconds"
		exit 1
	fi
	sleep 1
	retry=$((retry + 1))
	ACTUAL_ZPOOL_COUNT=$(pfexec zlogin oxz_switch /opt/oxide/omdb/bin/omdb db zpool list -i | wc -l)
done

# The bootstrap command creates a disk, so before that: adjust the control plane
# storage buffer to 0 as the virtual hardware only creates 20G pools

pfexec zlogin oxz_switch /opt/oxide/omdb/bin/omdb db zpool list

for ZPOOL in $(pfexec zlogin oxz_switch /opt/oxide/omdb/bin/omdb db zpool list -i);
do
	pfexec zlogin oxz_switch /opt/oxide/omdb/bin/omdb -w db zpool set-storage-buffer "${ZPOOL}" 0
done

pfexec zlogin oxz_switch /opt/oxide/omdb/bin/omdb db zpool list

export RUST_BACKTRACE=1
export E2E_TLS_CERT IPPOOL_START IPPOOL_END
eval "$(./target/debug/bootstrap)"
export OXIDE_HOST OXIDE_TOKEN

#
# The Nexus resolved in `$OXIDE_RESOLVE` is not necessarily the same one that we
# successfully talked to in bootstrap, so wait a bit for it to fully come online.
#
retry=0
while ! curl -sSf "$OXIDE_HOST/v1/ping" --resolve "$OXIDE_RESOLVE" --cacert "$E2E_TLS_CERT"; do
	if [[ $retry -gt 60 ]]; then
		echo "$OXIDE_RESOLVE failed to come up after 60 seconds"
		exit 1
	fi
	sleep 1
	retry=$((retry + 1))
done

/usr/oxide/oxide --resolve "$OXIDE_RESOLVE" --cacert "$E2E_TLS_CERT" \
	project create --name images --description "some images"
/usr/oxide/oxide \
    --resolve "$OXIDE_RESOLVE" \
    --cacert "$E2E_TLS_CERT" \
	disk import \
	--path debian-11-genericcloud-amd64.raw \
	--disk debian11-boot \
	--project images \
	--description "debian 11 cloud image from distros" \
	--snapshot debian11-snapshot \
	--image debian11 \
	--image-description "debian 11 original base image" \
	--image-os debian \
	--image-version "11" \
	--parallelism 1
/usr/oxide/oxide --resolve "$OXIDE_RESOLVE" --cacert "$E2E_TLS_CERT" \
	image promote --project images --image debian11

for test_bin in tests/*; do
	./"$test_bin"
done
