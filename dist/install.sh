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
for i in /opt/solarsanweb /opt/solarsanweb/lib /opt/solarsanweb/solarsanweb /opt/solarsan/lib /opt/solarsan /opt/solarsan/solarsan; do
    add2virtualenv | egrep "^$i\$" >/dev/null 2>&1 || \
        add2virtualenv "$i"
done
set -e

pip install -r /opt/solarsan/dist/requirements/dev-trevorj.pip
cp -v /opt/solarsan/dist/upstart/* /etc/init/

pushd /opt/solarsan/dist/

### All of this crap below and probably alot of the above should be in salt. ###

# TODO PPAs
#echo "[$0] Installing PPAs.."
#cp -v pkgs/ppas/*.list /etc/apt/sources.list.d/

# TODO install collectd
#apt-get -y install collectd collectd-utils
#cp -v collectd/*.conf /etc/collectd/

# TODO install nginx (not needed yet)
#apt-get -y install nginx-extras
#ln -s nginx/*.conf /etc/nginx/conf.d/solarsan.conf
#rm -f /etcc/nginx/sites-available/*

# TODO put scst in DKMS as a pkg in locsol solarsan apt repo
#svn checkout blah ~/build/scst
#cp -v scst/scst.conf /etc/scst.conf

popd

# This is what tells us we don't need to be installed by salt
echo "[$0] Success!"
touch /opt/solarsan/.installed
