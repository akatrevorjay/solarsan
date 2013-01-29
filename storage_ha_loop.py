#!/usr/bin/env python

from solarsan.core import logger
from solarsan import conf

#from solarsan.utils.stack import get_current_func_name
#from solarsan.utils.cache import cached_property
#from storage.drbd import DrbdResource
import time


def main():
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

            if p.is_online:
                try:
                    pools = p.pools()
                except:
                    pools = []
            else:
                try:
                    p.storage.root.peer_ping()
                    logger.warning('Peer "%s" came back up on ping!', p_host)

                    # TODO this doesn't really belong here. Should subclass
                    # rpyc client to do this and retries like I did on zerorpc
                    # in client_zerorpc
                    p._is_online = True
                except Exception, e:
                    logger.error('Peer "%s" is still down: "%s"', p_host, e.message)

                    # WHY IS THIS NEEDED when "stream has been closed"
                    if p.storage:
                        p.storage.close()

                    if p.is_local:
                        logger.error("For some reason the box I am on is down? Why am I still here?")
                        """ MAYBE CHECK LOCAL SERVICE STATUS? RESTART IF NEEDED? """
                    else:
                        if not local.is_primary:
                            logger.error('Failing over for Peer "%s"', p_host)

                            logger.error('artificial sleep')
                            time.sleep(5)

                            local.failover_for_peer(p)

                            logger.error('artificial sleep')
                            time.sleep(5)
                continue

            for pool in pools:
                try:
                    if not p.storage.root.pool_is_healthy(pool):
                        logger.error('Pool "%s" is NOT healthy on "%s".', pool, p_host)
                        # TODO signal for pool becoming unhealthy on RPC service
                    else:
                        logger.debug('Pool "%s" is healthy on "%s".', pool, p_host)
                        # TODO signal for pool becoming healthy on RPC service
                except Exception, e:
                    logger.error('Peer "%s" error on requesting Pool "%s" health: "%s"', p_host, pool, e.message)

        time.sleep(1)


if __name__ == '__main__':
    main()
