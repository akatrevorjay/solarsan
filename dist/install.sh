#!/bin/bash -e

### All of this crap below and probably alot of the above should be in salt. ###

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
for i in /opt/solarsanweb /opt/solarsanweb/lib /opt/solarsanweb/solarsanweb /opt/solarsan/lib /opt/solarsan /opt/solarsan/solarsan; do
    add2virtualenv | egrep "^$i\$" >/dev/null 2>&1 || \
        add2virtualenv "$i"
done
set -e

DIST="/opt/solarsan/dist"
pushd "$DIST"

echo "[$0] Installing PPAs and packages.."
./pkgs/install.sh

echo "[$@] Installing python packages.."
pip install -r /opt/solarsan/dist/requirements/dev-trevorj.pip

echo "[$@] Installing upstart scripts.."
cp -v /opt/solarsan/dist/upstart/* /etc/init/

echo "[$0] Installing configs.."
cp -v collectd/*.conf /etc/collectd/

cp -v nginx/*.conf /etc/nginx/conf.d/
#rm -f /etc/nginx/sites-enabled/*

# TODO put scst in DKMS as a pkg in locsol solarsan apt repo
cp -v scst/scst.conf /etc/scst.conf

popd

# This is what tells us we don't need to be installed by salt
echo "[$0] Success!"
touch /opt/solarsan/.installed
