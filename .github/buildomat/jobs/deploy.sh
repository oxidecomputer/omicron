#!/bin/bash
#:
#: name = "helios / deploy"
#: variety = "basic"
#: target = "lab-netdev"
#: output_rules = [
#:	"%/var/svc/log/system-illumos-sled-agent:default.log",
#:	"%/zone/oxz_nexus/root/var/svc/log/system-illumos-nexus:default.log",
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
ptime -m pfexec ./tools/create_virtual_hardware.sh

#
# XXX Right now, the Nexus external API is available on a specific IPv4 address
# on a canned subnet.  We need to create an address in the global zone such
# that we can, in the test below, reach Nexus.
#
# This must be kept in sync with the IP in "smf/sled-agent/config-rss.toml" and
# the prefix length which apparently defaults (in the Rust code) to /24.
#
pfexec ipadm create-addr -T static -a 192.168.1.199/24 igb0/sidehatch

#
# This OMICRON_NO_UNINSTALL hack here is so that there is no implicit uninstall
# before the install.  This doesn't work right now because, above, we made
# /var/oxide a file system so you can't remove it (EBUSY) like a regular
# directory.  The lab-netdev target is a ramdisk system that is always cleared
# out between runs, so it has not had any state yet that requires
# uninstallation.
#
OMICRON_NO_UNINSTALL=1 \
    ptime -m pfexec ./target/release/omicron-package install

# Wait up to 5 minutes for RSS to say it's done
for _i in {1..30}; do
	sleep 10
	grep "Finished setting up services" "$(svcs -L sled-agent)" && break
done

set +o xtrace

start=$SECONDS
while :; do
	if (( SECONDS - start > 60 )); then
		printf 'FAILURE: NEXUS DID NOT BECOME AVAILABLE\n' >&2
		exit 1
	fi

	if curl --fail-with-body -i http://192.168.1.20/spoof_login; then
		printf 'ok; nexus became available!\n'
		break
	fi

	sleep 1
done

#
# XXX add tests here!
#
