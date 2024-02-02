#!/usr/bin/env bash

./target/debug/nexus nexus/examples/config.toml -O > openapi/nexus.json
./target/debug/nexus nexus/examples/config.toml -I > openapi/nexus-internal.json
