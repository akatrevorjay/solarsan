#!/bin/bash
#exec >>/tmp/log 2>&1
#echo "[$0] $*"
cd /opt/solarsan
exec ./bin/env ./bin/cli "$@"
