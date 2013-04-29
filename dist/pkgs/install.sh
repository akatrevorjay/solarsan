#!/bin/bash

CODENAME=$(lsb_release -sc)

for ppa in $(cat ppas); do
    ppa_list=$(echo "$ppa" | sed -e 's|/|-|g; s|\.|_|g; s|^ppa:||')
    ppa_list="/etc/apt/sources.list.d/${ppa_list}-${CODENAME}.list"

    echo "ppa=$ppa ppa_list=$ppa_list"

    [[ -f "$ppa_list" ]] \
        || apt-add-repository -y "$ppa"
done

apt-get update
#apt-get dist-upgrade -y

cat pkgs \
    | egrep -v '^#' \
    | xargs apt-get install -y
