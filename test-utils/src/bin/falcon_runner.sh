#!/bin/bash

if [[ "$*" == *--list* ]]; then
  exec "$@"
else
   ##cargo build --bin falcon_runner
   # Nextest appears to set this when run from the omicron dir
   pfexec $OLDPWD/target/debug/falcon_runner $@
fi
