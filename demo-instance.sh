#!/bin/bash
set -eu

trap "kill 0" EXIT

set -x
# ./tools/oxapi_demo disk_create_demo         myorg myproject mydisk
./tools/oxapi_demo instance_create_demo     myorg myproject myinstance
# ./tools/oxapi_demo instance_attach_disk     myorg myproject myinstance mydisk
# ./tools/oxapi_demo instance_start           myorg myproject myinstance
set +x
