#!/usr/bin/env python

import logging
from django.conf import settings
import zerorpc

#from configure.models import Nic
#from storage.models import Pool


class SolarSanRPC(object):
    """
    System
    """

    def hostname(self):
        """Get hostname"""
        return settings.SERVER_NAME

    ## TODO Broken
    #def interfaces(self):
    #    """Get interfaces and their addresses"""
    #    ifaces = Nic.list()
    #    ret = {}
    #    for iface_name, iface in ifaces.iteritems():
    #        for af, addrs in iface.addrs.iteritems():
    #            for addr in addrs:
    #                if 'addr' not in addr or 'netmask' not in addr:
    #                    continue
    #                if iface_name not in ret:
    #                    ret[iface_name] = {}
    #                if af not in ret[iface_name]:
    #                    ret[iface_name][af] = []
    #                ret[iface_name][af].append((addr['addr'], addr['netmask']))
    #    return ret

    def ping(self):
        """Ping Peer"""
        return True


class StorageRPC(object):
    """
    Devices
    """

    def device_list(self):
        """List Devices"""
        pass

    """
    Pools
    """

    def pool_create(self, name, vdevs):
        """Create Pool"""
        pass

    #def pool_list(self):
    #    """List Pools"""
    #    return [pool.name for pool in Pool.objects.all()]

    #def pool_is_healthy(self, name):
    #    """Check if Pool is healthy"""
    #    pool = Pool.objects.get(name=name)
    #    return pool.is_healthy()

    #def pool_children_list(self, name):
    #    """List Pool children"""
    #    pool = Pool.objects.get(name=name)
    #    return pool.children


SOCK_DIR = '/opt/solarsan/rpc/sock'


def get_sock_path(name):
    return 'ipc://%s/%s' % (SOCK_DIR, name)


def run_server():
    logging.info("Starting SolarSan RPC Server..")
    s = zerorpc.Server(SolarSanRPC())
    s.bind(get_sock_path('solarsan'))
    s.run()


def main():
    run_server()


if __name__ == '__main__':
    main()
