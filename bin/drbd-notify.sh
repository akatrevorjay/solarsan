#!/bin/bash
#exec >>/tmp/log 2>&1
#echo "[$0] $*"
cd /opt/solarsan
./bin/env ./bin/drbd-notify "$@" || logger -t solarsan.drbd-notify -p err "Could not run drbd-notify with '$*'."
