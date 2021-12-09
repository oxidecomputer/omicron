#!/bin/bash
set -eu

trap "kill 0" EXIT

if { [ "$TERM" = "screen" ] && [ -n "$TMUX" ]; } then
  echo "This script should be run in tmux! It splits panes..."
  exit 1
fi

# First of all, split the lower half of the screen into logs.
services=(nexus sled-agent oximeter)

pane_height=$(tmux list-pane -F "#{pane_id}:#{pane_height}" | rg "$TMUX_PANE:" | cut -d: -f2)
pane_count=$(("${#services[@]}" + 1 ))
height_per_svc=$(( "$pane_height" / "$pane_count" ))
height_remaining=$(( "$pane_height" - "$height_per_svc" ))

pane="$TMUX_PANE"
for index in "${!services[@]}"; do
  service="${services[index]}"
  logfile="$(svcs -L "$service")"
  pane=$(tmux split-window \
    -t "$pane" \
    -l "$height_remaining" \
    -P -F "#{pane_id}" \
    -v \
    "tail -F $logfile")

  height_remaining=$(( "$height_remaining" - "$height_per_svc" ))
done

# Then just keep watching the services.
watch -n 2 "svcs -a | rg illumos"
