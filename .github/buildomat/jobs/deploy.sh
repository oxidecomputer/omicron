#!/bin/bash
#:
#: name = "helios / deploy"
#: variety = "basic"
#: target = "lab-2.0-opte-0.23"
#: output_rules = [
#:  "%/var/svc/log/oxide-sled-agent:default.log*",
#:  "%/pool/ext/*/crypt/zone/oxz_*/root/var/svc/log/oxide-*.log*",
#:  "%/pool/ext/*/crypt/zone/oxz_*/root/var/svc/log/system-illumos-*.log*",
#:  "!/pool/ext/*/crypt/zone/oxz_propolis-server_*/root/var/svc/log/*.log*"
#:  "%/pool/ext/*/crypt/debug/global/oxide-sled-agent:default.log.*",
#:  "%/pool/ext/*/crypt/debug/oxz_*/oxide-*.log.*",
#:  "%/pool/ext/*/crypt/debug/oxz_*/system-illumos-*.log.*",
#:  "!/pool/ext/*/crypt/debug/oxz_propolis-server_*/*.log.*"
#: ]
#: skip_clone = true
#:
#: [dependencies.package]
#: job = "helios / package"
#:
#: [dependencies.ci-tools]
#: job = "helios / CI tools"

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
	pfexec zlogin sidecar_softnpu /softnpu/scadm \
		--server /softnpu/server \
		--client /softnpu/client \
		standalone \
		dump-state
	pfexec /opt/oxide/opte/bin/opteadm list-ports
	z_swadm link ls
	z_swadm addr list
	z_swadm route list
	z_swadm arp list

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

	pfexec zlogin sidecar_softnpu cat /var/log/softnpu.log

	exit $status
}
trap _exit_trap EXIT

z_swadm () {
	pfexec zlogin oxz_switch /opt/oxide/dendrite/bin/swadm $@
}

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
cp /input/package/work/zones/* out/
mkdir tests
for p in /input/ci-tools/work/end-to-end-tests/*.gz; do
	ptime -m gunzip < "$p" > "tests/$(basename "${p%.gz}")"
	chmod a+x "tests/$(basename "${p%.gz}")"
done

# Lab gateway ip
export GATEWAY_IP=192.168.1.199
# Proxy arp settings.
export PXA_START="192.168.1.50"
export PXA_END="192.168.1.90"

# Nexus (and any instances using the above IP pool) are configured to use external
# IPs from a fixed subnet (192.168.1.0/24). OPTE/SoftNPU/Boundary Services take care
# of NATing between the private VPC networks and this "external network".
# We create a static IP in this subnet in the global zone and configure the switch
# to use it as the default gateway.
# NOTE: Keep in sync with $[SERVICE_]IP_POOL_{START,END}
pfexec ipadm create-addr -T static -a $GATEWAY_IP/24 igb0/sidehatch

pfexec zpool create -f scratch c1t1d0 c2t1d0
ZPOOL_VDEV_DIR=/scratch ptime -m pfexec ./tools/create_virtual_hardware.sh

#
# Generate a self-signed certificate to use as the initial TLS certificate for
# the recovery Silo.  Its DNS name is determined by the silo name and the
# delegated external DNS name, both of which are in the RSS config file.  In a
# real system, the certificate would come from the customer during initial rack
# setup on the technician port.
#
tar xf out/omicron-sled-agent.tar pkg/config-rss.toml
SILO_NAME="$(sed -n 's/silo_name = "\(.*\)"/\1/p' pkg/config-rss.toml)"
EXTERNAL_DNS_DOMAIN="$(sed -n 's/external_dns_zone_name = "\(.*\)"/\1/p' pkg/config-rss.toml)"
rm -f pkg/config-rss.toml

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
# The lab-netdev target is a ramdisk system that is always cleared
# out between runs, so it has not had any state yet that requires
# uninstallation.
#
OMICRON_NO_UNINSTALL=1 \
    ptime -m pfexec ./target/release/omicron-package -t test install

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


# We also need to configure proxy arp for any services which use OPTE for external connectivity (e.g. Nexus)
tar xf out/omicron-sled-agent.tar pkg/config-rss.toml
SOFTNPU_MAC=$(dladm show-vnic sc0_1 -p -o macaddress | sed -E 's/[ :]/&0/g; s/0([^:]{2}(:|$))/\1/g')
SERVICE_IP_POOL_START="$(sed -n 's/^first = "\(.*\)"/\1/p' pkg/config-rss.toml)"
SERVICE_IP_POOL_END="$(sed -n 's/^last = "\(.*\)"/\1/p' pkg/config-rss.toml)"
rm -r pkg

pfexec zlogin sidecar_softnpu /softnpu/scadm \
	--server /softnpu/server \
	--client /softnpu/client \
	standalone \
	add-proxy-arp $SERVICE_IP_POOL_START $SERVICE_IP_POOL_END $SOFTNPU_MAC

pfexec zlogin sidecar_softnpu /softnpu/scadm \
	--server /softnpu/server \
	--client /softnpu/client \
	standalone \
	dump-state

export RUST_BACKTRACE=1
export E2E_TLS_CERT
./tests/bootstrap

rm ./tests/bootstrap
for test_bin in tests/*; do
	./"$test_bin"
done
