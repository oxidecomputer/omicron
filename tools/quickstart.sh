#!/bin/bash

set -e
set -x

ip_pool_start=10.85.0.210
ip_pool_end=10.85.0.230
# This should be the same as the mac address on vnic sc0_1,  set by
# ./tools/create_virtual_hardware.sh
# Check with `dladm show-vnic sc0_1`
softnpu_mac=a8:e1:de:01:70:1d

# Create an org
# check for myorg, create if absent
oxide org view myorg || oxide org create myorg

# Create a project
# check for myproj, create if absent
oxide project view myproj -o myorg || oxide project create -o myorg myproj

# Add ip range to default pool
# Check for existing range
(oxide api /system/ip-pools/default/ranges | jq '.items[0].range.first' | grep null) && {
  # Add range if none is present
  oxide api /system/ip-pools/default/ranges/add --method POST --input - <<EOF
{
  "first": "$ip_pool_start",
  "last": "$ip_pool_end"
}
EOF
}

# Add proxy-arp for ip-pool if you're working from the same L2 network
# as your development machine
pfexec /opt/oxide/softnpu/stuff/scadm \
  --server /opt/oxide/softnpu/stuff/server \
  --client /opt/oxide/softnpu/stuff/client \
  standalone \
  add-proxy-arp \
  $ip_pool_start \
  $ip_pool_end \
  $softnpu_mac

# Create disk
# NOTE: This requires that your images already be populated
oxide api /system/images/debian-nocloud || {
  echo "cannot proceed, images not populated!"; exit 1
}

# Check for disk
oxide api /organizations/myorg/projects/myproj/disks/debian || {
  # Create if absent
  oxide api /organizations/myorg/projects/myproj/disks/ --method POST --input - <<EOF
{
  "name": "debian",
  "description": "debian nocloud",
  "block_size": 512,
  "size": 2147483648,
  "disk_source": {
      "type": "global_image",
      "image_id": "$(oxide api /system/images/debian-nocloud | jq -r .id)"
  }
}
EOF
}

# Create instance
# check for myinst
oxide api /organizations/myorg/projects/myproj/instances/myinst || {
   # Create if absent
  oxide api /organizations/myorg/projects/myproj/instances --method POST --input - <<EOF
{
  "name": "myinst",
  "description": "my inst",
  "hostname": "myinst",
  "memory": 1073741824,
  "ncpus": 2,
  "disks": [
    {
      "type": "attach",
      "name": "debian"
    }
  ],
  "external_ips": [{"type": "ephemeral"}]
}
EOF
}
