#!/usr/bin/env python

#from ..core import logger
from solarsan.core import logger
from solarsan import conf

#from solarsan.utils.stack import get_current_func_name
#from solarsan.utils.cache import cached_property
#from storage.drbd import DrbdResource
import time


def storage_pool_health_loop():
    hostname = conf.hostname
    from cluster.models import Peer

    peers = {}
    local = None
    for peer in Peer.objects.all():
        p_hostname = peer.hostname
        peers[p_hostname] = peer
        if peers[p_hostname].is_local:
            if local:
                raise Exception('Found two Peers with my hostname!')
            local = peers[hostname]

    #for p_host, p in peers.iteritems():
    #    logger.info('Peer "%s" pools found: "%s".', p_host, p.pools.keys())
    #    # TODO get hostname of peer for each replicated volume
    #    logger.info('Peer "%s" replicated volumes found: "%s".', p_host, p.cvols)

    while True:
        for p_host, p in peers.iteritems():
            print
            logger.debug('Peer "%s"' % p_host)

            if not p.is_online:
                try:
                    p.storage.root.peer_ping()
                    logger.warning('Peer "%s" came back up!', p_host)
                except Exception, e:
                    logger.error('Peer "%s" is still down: "%s"', p_host, e.message)
                    continue

            for pool in p.pools:
                try:
                    if not p.storage.root.pool_is_healthy(pool):
                        logger.error('Pool "%s" is NOT healthy on "%s".', pool, p_host)
                        # TODO signal for pool becoming unhealthy on RPC service
                    else:
                        logger.debug('Pool "%s" is healthy on "%s".', pool, p_host)
                        # TODO signal for pool becoming healthy on RPC service
                except Exception, e:
                    logger.error('Peer "%s" went down: "%s"', p_host, e.message)

                    # TODO signal when peer goes online/offline, handle this
                    # through that:

                    # TODO rpyc of objects and per peer filter
                    for res in local('drbd_res_list'):
                        # TODO signal going primary on res
                        local('drbd_primary', res)
                    # TODO rewrite SCST config
                    # TODO signal about to reload SCST config
                    if not local('target_scst_status'):
                        local('target_scst_start')
                    else:
                        local('target_scst_reload_config')

                    # TODO HA IP lookup from Config, use signals to run this
                    # shit on down or up

        time.sleep(1)
