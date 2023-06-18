#!/bin/bash
# Populate an Oxide host running Omicron with images from server catacomb.
#
# Note that the default tunnel IP of `fd00:...` will only be available _after_
# launching the control plane with `omicron-package install`, since Omicron
# creates that address.

set -eu
CATACOMB_TUNNEL="${CATACOMB_TUNNEL:-"[fd00:1122:3344:101::1]:54321"}"
echo "Populating debian"
oxide api /v1/images --method POST --input - <<EOF
{
  "name": "debian",
  "description": "debian",
  "block_size": 512,
  "distribution": {
    "name": "debian",
    "version": "11"
  },
  "source": {
      "type": "url",
      "url": "http://${CATACOMB_TUNNEL}/media/cloud/debian-11-genericcloud-amd64.raw"
  }
}
EOF

echo "Populating ubuntu"
oxide api /v1/images --method POST --input - <<EOF
{
  "name": "ubuntu",
  "description": "Ubuntu",
  "block_size": 512,
  "distribution": {
    "name": "ubuntu",
    "version": "22.04"
  },
  "source": {
      "type": "url",
      "url": "http://${CATACOMB_TUNNEL}/media/cloud/focal-server-cloudimg-amd64.raw"
  }
}
EOF

echo "Populating fedora"
oxide api /v1/images --method POST --input - <<EOF
{
  "name": "fedora",
  "description": "fedora",
  "block_size": 512,
  "distribution": {
    "name": "fedora",
    "version": "35-1.2"
  },
  "source": {
      "type": "url",
      "url": "http://${CATACOMB_TUNNEL}/media/cloud/Fedora-Cloud-Base-35-1.2.x86_64.raw"
  }
}
EOF

echo "Populating debian-nocloud"
oxide api /v1/images --method POST --input - <<EOF
{
  "name": "debian-nocloud",
  "description": "debian nocloud",
  "block_size": 512,
  "distribution": {
    "name": "debian-nocloud",
    "version": "nocloud 11"
  },
  "source": {
      "type": "url",
      "url": "http://${CATACOMB_TUNNEL}/media/debian/debian-11-nocloud-amd64-20220503-998.raw"
  }
}
EOF

echo "Populating ubuntu-iso"
oxide api /v1/images --method POST --input - <<EOF
{
  "name": "ubuntu-nocloud-iso",
  "description": "ubuntu nocloud iso",
  "block_size": 512,
  "distribution": {
    "name": "ubuntu-iso",
    "version": "iso 22.04"
  },
  "source": {
      "type": "url",
      "url": "http://${CATACOMB_TUNNEL}/media/ubuntu/ubuntu-22.04-live-server-amd64.iso"
  }
}
EOF

echo "Populating windows"
oxide api /v1/images --method POST --input - <<EOF
{
  "name": "windows-server-2022",
  "description": "Windows Server 2022",
  "block_size": 512,
  "distribution": {
    "name": "windows-server",
    "version": "2022"
  },
  "source": {
      "type": "url",
      "url": "http://${CATACOMB_TUNNEL}/media/cloud/windows-server-2022-genericcloud-amd64.raw"
  }
}
EOF
