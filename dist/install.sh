#!/bin/bash

. /etc/bash.bashrc
. /etc/bash_completion

if [[ ! -e "/root/.virtualenvs/solarsan" ]]; then
    mkvirtualenv --no-site-packages --distribute -v solarsan
fi

workon solarsan
for i in /opt/solarsan{,/solarsan,web{,/lib,/solarsanweb}}; do
    add2virtualenv | egrep "^$i\$" >/dev/null 2>&1 || \
        add2virtualenv "$i"
done


# This is what tells us we don't need to be installed by salt
touch /opt/solarsan/.installed
