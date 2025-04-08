#!/usr/bin/env bash

./target/debug/sled-agent openapi bootstrap > openapi/bootstrap-agent.json
./target/debug/sled-agent openapi sled > openapi/sled-agent.json
