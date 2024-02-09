#!/usr/bin/env bash
# Simple script to install the alpine image included with propolis.

if ! oxide api /v1/images > /dev/null; then
    echo "Problem detected running the oxide CLI"
    echo "Please install, set path, or setup authorization"
    exit 1
fi

oxide api /v1/images --method POST --input - <<EOF
{
  "name": "alpine",
  "description": "boot from propolis zone blob!",
  "block_size": 512,
  "distribution": {
    "name": "alpine",
    "version": "propolis-blob"
  },
  "source": {
    "type": "you_can_boot_anything_as_long_as_its_alpine"
  }
}
EOF
if [[ $? -ne 0 ]]; then
        echo "There was a problem installing the alpine image"
    echo "Please check Nexus logs for possible clues"
    echo "pfexec zlogin oxz_nexus_<UUID> \"tail \\\$(svcs -L nexus)\""
    exit 1
fi
