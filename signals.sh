#!/bin/bash

set -o xtrace
pfexec /usr/sbin/dtrace \
    -x switchrate=1000hz \
    -q \
    -o /var/tmp/signals.log \
    -n '
        signal-send {
                printf("%Y (pid %d, %s) send signal %d (to pid %d, %s)\n",
                    walltimestamp, pid, execname, args[2],
                    args[1]->pr_pid, args[1]->pr_fname);
                ustack();
        }
    ' >/dev/null 2>&1 </dev/null &
set +o xtrace

printf 'dtrace started with pid %d\n' "$!" >&2
disown

