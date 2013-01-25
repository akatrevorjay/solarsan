#!/usr/bin/env python

import logging
#from django.conf import settings
import zerorpc

from solarsan.utils import LoggedException
#from configure.models import Nic
#from storage.models import Pool

from storage.pool import Pool
from storage.dataset import Volume

#import sh
import drbd


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
    DRBD
    """

    def cluster_volume_drbd_status(self):
        """Get status of all DRBD replicated resources"""
        return drbd.status()

    def cluster_volume_setup(self, volume, peer):
        """Setup synchronous replication on an existing volume with a cluster peer."""
        vol = Volume(name=volume)
        #cvol = CVolume(name=volume, peer=peer)
        pass


SOCK_DIR = '/opt/solarsan/rpc/sock'


def get_sock_path(name):
    return 'ipc://%s/%s' % (SOCK_DIR, name)


def run_server():
    logging.info("Starting Storage RPC Server..")
    s = zerorpc.Server(StorageRPC())
    s.bind(get_sock_path('storage'))
    s.run()


def main():
    run_server()


if __name__ == '__main__':
    main()
