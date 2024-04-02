#!/bin/bash

# Override on the command line for your path to Omicron
OMICRON=${OMICRON:-~/omicron}

if [[ "$*" == *--list* ]]; then
  exec "$@"
else
   cargo build -p omicron-test-utils --bin falcon_runner
   pfexec $OMICRON/target/debug/falcon_runner $@
fi
