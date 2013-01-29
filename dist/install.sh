#!/bin/bash -e

export HOME="/root"
export WORKON_HOME="/root/.virtualenvs"

set +e
. /etc/bash.bashrc
. /etc/bash_completion
set -e

if [[ ! -e "/root/.virtualenvs/solarsan" ]]; then
    mkvirtualenv --no-site-packages --distribute -v solarsan
fi

set +e
workon solarsan
for i in /opt/solarsanweb /opt/solarsanweb/lib /opt/solarsanweb/solarsanweb /opt/solarsan /opt/solarsan/solarsan; do
    add2virtualenv | egrep "^$i\$" >/dev/null 2>&1 || \
        add2virtualenv "$i"
done
set -e

pip install -r /opt/solarsan/dist/requirements/dev-trevorj.pip

# This is what tells us we don't need to be installed by salt
echo "[$0] Success!"
touch /opt/solarsan/.installed
