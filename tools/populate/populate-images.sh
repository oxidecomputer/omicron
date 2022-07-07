#!/bin/bash
# Populate an Oxide lab host running Omicron with images from server catacomb.

set -eu
CATACOMB_TUNNEL="[fd00:1122:3344:101::1]:54321"
res=0
echo "Populating debian"
oxide api /images --method POST --input - <<EOF
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
      "url": "http://${CATACOMB_TUNNEL}/media/debian/debian-11-nocloud-amd64-20220503-998.raw"
  }
}
EOF

echo "Populating focal"
oxide api /images --method POST --input - <<EOF
{
  "name": "focal",
  "description": "focal",
  "block_size": 512,
  "distribution": {
    "name": "focal",
    "version": "1"
  },
  "source": {
      "type": "url",
      "url": "http://${CATACOMB_TUNNEL}/media/cloud/focal-server-cloudimg-amd64.raw"
  }
}
EOF

echo "Populating ubuntu"
oxide api /images --method POST --input - <<EOF
{
  "name": "ubuntu",
  "description": "ubuntu",
  "block_size": 512,
  "distribution": {
    "name": "ubuntu",
    "version": "22.04"
  },
  "source": {
      "type": "url",
      "url": "http://${CATACOMB_TUNNEL}/media/ubuntu/ubuntu-22.04-live-server-amd64.iso"
  }
}
EOF

# A sanity check to see if access is working to catacomb
curl_test=$(curl -w '%{http_code}\n' -s -o /dev/null http://${CATACOMB_TUNNEL}/media/test.txt)
if [[ $curl_test -eq 200 ]]; then
    echo "✔ Curl test to catacomb worked"
else
    echo "To use these images, you need to have network access"
    echo "to the catacomb lab system at ${CATACOMB_TUNNEL}"
    echo ""
    echo "For example, I run this from the system \"sock\" to set up a tunnel:"
    echo "ssh -L ${CATACOMB_TUNNEL}:catacomb:80 catacomb"
    echo "Leave that running in a different window while access is required"
    echo ""
    echo "Test of curl to catacomb failed"
    echo "curl http://${CATACOMB_TUNNEL}/media/test.txt"
    res=1
fi
exit $res
