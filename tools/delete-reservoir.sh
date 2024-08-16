#!/usr/bin/env bash

size=`pfexec /usr/lib/rsrvrctl -q | grep Free | awk '{print $3}'`
let x=$size/1024

pfexec /usr/lib/rsrvrctl -r $x
