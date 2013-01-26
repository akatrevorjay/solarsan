#!/usr/bin/env python

from solarsanweb.settings.base import _register_mongo_databases
from django.conf import settings


import logging
#from django.conf import settings
import zerorpc

#from solarsan.utils import LoggedException
from solarsan.models import Config
from configure.models import Nic, get_all_local_ipv4_addrs
#from configure.models import Nic
#from storage.models import Pool

from storage.pool import Pool
from storage.dataset import Volume
import storage.models as sm

import sh
import drbd

#from .client import get_client
#from cluster.models import Peer


class StorageRPC(object):
    def __init__(self):
        pass

    """
    Devices
    """

    #def device_list(self):
    #    """List Devices"""
    #    pass

    """
    Pools
    """

    #def pool_create(self, name, vdevs):
    #    """Create Pool"""
    #    pass

    def pool_list(self, props=None):
        """List Pools"""
        return Pool.list(props=props, ret=dict, ret_obj=False)

    def pool_exists(self, pool):
        """Check if Pool exists"""
        pool = Pool(name=pool)
        return pool.exists()

    def pool_is_healthy(self, pool):
        """Check if Pool is healthy"""
        pool = Pool(name=pool)
        return pool.is_healthy()

    def pool_get_prop(self, pool, name, default=None):
        """Get property from Pool"""
        pool = Pool(name=pool)
        return pool.properties.get(name, default)

    def pool_set_prop(self, pool, name, value):
        """Set property to Pool"""
        pool = Pool(name=pool)
        pool.properties[name] = value
        return True

    #def pool_children_list(self, name):
    #    """List Pool children"""
    #    pool = Pool.objects.get(name=name)
    #    return pool.children

    """
    Volumes
    """

    def volume_create(self, volume, size, sparse=False, block_size=None):
        """Create Volume"""
        vol = Volume(name=volume)
        return vol.create(size, sparse=sparse, block_size=block_size)

    def volume_list(self, props=None):
        """List Volumes"""
        return Volume.list(props=props, ret=dict, ret_obj=False)

    def volume_exists(self, volume):
        """Check if Volume exists"""
        vol = Volume(name=volume)
        return vol.exists()

    def volume_get_prop(self, volume, name, default=None, source=None):
        """Get property from Volume"""
        vol = Volume(name=volume)
        return vol.properties.get(name, default, source=source)

    def volume_set_prop(self, volume, name, value):
        """Set property to vol"""
        vol = Volume(name=volume)
        vol.properties[name] = value
        return True

    """
    Cluster Probe
    """

    def peer_ping(self):
        """Pings Peer"""
        return True

    def peer_get_cluster_iface(self):
        """Gets Cluster IP"""
        config = Config.objects.get(name='cluster')
        iface = config.network_iface
        nic = Nic(str(iface))
        #nic_config = nic.config._data
        #nic_config.pop(None)

        ret = {
            'name': nic.name,
            'ipaddr': nic.ipaddr,
            'netmask': nic.netmask,
            'type': nic.type,
            'cidr': nic.cidr,
            'mac': nic.mac,
            'mtu': nic.mtu,
            #'config': nic_config,
        }
        return ret

    def peer_list_addrs(self):
        """Returns a list of IP addresses and network information"""
        return get_all_local_ipv4_addrs()

    def peer_hostname(self):
        """Returns hostname"""
        return settings.SERVER_NAME

    """
    Cluster Volumes
    """

    def cluster_volume_status(self):
        """Get status of all DRBD replicated resources"""
        return drbd.status()

    def cluster_volume_list(self, hostname=None):
        """Lists Cluster Volumes"""
        ret = []
        for line in sh.zfs('get', '-s', 'local', '-H', '-t', 'volume', 'solarsan:cluster_volume',
                           _iter=True):
            line = line.rstrip("\n")
            name, prop, val, source = line.split("\t", 3)
            if val != 'true':
                logging.warning('Volume "%s" has cluster volume property of "%s", which is not "true". Ignoring property.')
                continue
            ret.append(name)
        return ret

    def cluster_volume_setup(self, volume, peer_hostname, is_source=True):
        """Setup synchronous replication on an existing volume with a cluster peer."""
        svol = sm.Volume(name=volume)
        return svol.replication_setup(is_source=is_source, peer_hostname=peer_hostname)


#SOCK_DIR = '/opt/solarsan/rpc/sock'


def get_sock_path(name):
    #return 'ipc://%s/%s' % (SOCK_DIR, name)
    return 'tcp://0.0.0.0:%d' % settings.CLUSTER_DISCOVERY_PORT


def run_server():
    logging.info("Starting Storage RPC Server..")
    s = zerorpc.Server(StorageRPC())
    s.bind(get_sock_path('storage'))
    s.run()


def main():
    run_server()


if __name__ == '__main__':
    _register_mongo_databases()
    main()
