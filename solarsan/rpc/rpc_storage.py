#!/usr/bin/env python

from solarsan.core import logger
#from solarsan.utils.exceptions import LoggedException

from storage.pool import Pool
from storage.dataset import Volume
from storage.parsers.drbd import drbd_overview_parser

from solarsan.models import Config
from configure.models import Nic, get_all_local_ipv4_addrs
from cluster.models import Peer

#from . import client
import solarsan.rpc.client as client

import zerorpc
import sh
from socket import gethostname


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

    def volume_create(self, volume, size, sparse=None, block_size=None):
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
        return gethostname()

    """
    Cluster Volumes
    """

    def volume_repl_status(self, volume=None):
        """Get status of all DRBD replicated resources"""
        if volume:
            vol = Volume(name=Volume)
            return vol.replication_status
        else:
            return drbd_overview_parser()

    def volume_repl_list(self, hostname=None):
        """Lists Cluster Volumes"""
        ret = []
        for line in sh.zfs('get', '-s', 'local', '-H', '-t', 'volume', 'solarsan:vol_repl',
                           _iter=True):
            line = line.rstrip("\n")
            name, prop, val, source = line.split("\t", 3)
            if val != 'true':
                logger.warning('Volume "%s" has vol_repl="%s" (not "true"). Ignoring property.')
                continue
            ret.append(name)
        return ret

    def volume_repl_setup(self, volume, peer_volume, size, peer_hostname, is_source=None):
        """Create new volume and setup Setup synchronous replication with a cluster peer."""
        peer = Peer.objects.get(hostname=peer_hostname)
        p_hostname = peer.hostname
        p_cluster_addr = peer.cluster_addr

        hostname = gethostname()
        local = client.Host(hostname, 'localhost')
        remote = client.Host(p_hostname, p_cluster_addr)

        # Create volumes
        local('volume_create', volume, size)
        local('volume_set_prop', volume, 'solarsan:vol_repl', 'pending')
        remote('volume_create', peer_volume, size)
        remote('volume_set_prop', peer_volume, 'solarsan:vol_repl', 'pending')

        # Set properties
        local('volume_set_prop', volume, 'solarsan:vol_repl_peer', remote.hostname)
        remote('volume_set_prop', peer_volume, 'solarsan:vol_repl_peer', local.hostname)

        # TODO Use UUID per vol and peer

        # Write DRBD config on each box
        local('volume_repl_write_config', volume)
        remote('volume_repl_write_config', peer_volume)

        #return svol.replication_setup(is_source=is_source, peer_hostname=peer_hostname)

    def volume_repl_write_config(self, volume):
        pass

    """
    Target
    """

    def target_scst_status(self):
        return sh.service('scst', 'status')

    def target_scst_start(self):
        return sh.service('scst', 'start')

    def target_scst_stop(self):
        return sh.service('scst', 'stop')

    #def target_rts_status(self):
    #    pass


#SOCK_DIR = '/opt/solarsan/rpc/sock'


def get_sock_path(name):
    #return 'ipc://%s/%s' % (SOCK_DIR, name)
    return 'tcp://0.0.0.0:%d' % 1785


def run_server():
    logger.info("Starting Storage RPC Server..")
    s = zerorpc.Server(StorageRPC())
    s.bind(get_sock_path('storage'))
    s.run()


def main():
    run_server()


if __name__ == '__main__':
    main()
