#!/bin/bash
#:
#: name = "helios / deploy"
#: variety = "basic"
#: target = "lab-netdev"
#: output_rules = [
#:	"/var/oxide/sled-agent.log",
#: ]
#: skip_clone = true
#:

set -o errexit
set -o pipefail
set -o xtrace

#
# XXX work around 14537 (UFS should not allow directories to be unlinked) which
# is probably not yet fixed in xde branch?
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
# XXX the creation of rpool/zone is perhaps not working as expected?
#
/sbin/zfs create -o mountpoint=/zone rpool/zone

#
# The sled agent will ostensibly write things into /var/oxide, so make that a
# tmpfs as well:
#
pfexec mkdir -p /var/oxide
pfexec mount -F tmpfs -O swap /var/oxide

pfexec mkdir /opt/oxide/work
pfexec chown build:build /opt/oxide/work
cd /opt/oxide/work

#
# XXX download package to avoid rebuild for quicker cycles
#
pfexec mkdir -p /input/package/work
pfexec chown build:build /input/package/work
curl -sSf -L -o /input/package/work/package.tar.gz \
    'https://buildomat.eng.oxide.computer/wg/0/artefact/01G3QWB2R0941K1BWG4P2QC6SH/b3V6e1UxIR3zLnjuhPnQTf0GwwqbHTkf0IiZaonKEvwiIvbt/01G3QWBA68V43JMD72AVECB1Q5/01G3QXE4CZGNND81NAHA52RBEV/package.tar.gz'

ptime -m tar xvzf /input/package/work/package.tar.gz
ptime -m pfexec ./tools/create_virtual_hardware.sh
OMICRON_NO_UNINSTALL=1 \
    ptime -m pfexec ./target/release/omicron-package install

# Wait up to 5 minutes for RSS to say it's done
for _i in {1..30}; do
	sleep 10
	grep 'Finished setting up services' /var/oxide/sled-agent.log && break
done

# TODO: write tests and run the resulting test bin here
curl -i http://[fd00:1122:3344:0101::3]:12220

ptime -m pfexec ./target/release/omicron-package uninstall
ptime -m pfexec ./tools/destroy_virtual_hardware.sh
