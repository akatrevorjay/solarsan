#!/usr/bin/env python

#from ..core import logger
from solarsan.core import logger
from solarsan import conf

import zerorpc
from solarsan.utils.stack import get_current_func_name
#from solarsan.utils.cache import cached_property
#from storage.drbd import DrbdResource
import time


class ClientWithRetry(zerorpc.Client):
    """Wrapper of zerorpc.Client to automatically retry failed
    requests."""
    _retry_attempts = 1
    _retry_delay = 1

    def __init__(self, *args, **kwargs):
        for i in ['retry_attempts', 'retry_delay']:
            if i in kwargs:
                setattr(self, '_%s' % i, kwargs.pop(i))
        zerorpc.Client.__init__(self, *args, **kwargs)

    def __call__(self, method, *args, **kwargs):
        retry_attempts = kwargs.pop('_retry_attempts', self._retry_attempts)
        retry_delay = kwargs.pop('_retry_delay', self._retry_delay)
        for attempt in xrange(0, retry_attempts + 1):
            try:
                ret = zerorpc.Client.__call__(self, method, *args, **kwargs)
                return ret
            except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
                log_msg = 'Connection error: "%s".' % e
                if attempt >= retry_attempts:
                    # raise exception on last attempt
                    logger.debug('%s Retry count exeeded (%d/%d), giving up.',
                                 log_msg, attempt, retry_attempts)
                    raise
                else:
                    # try again a little later
                    logger.debug('%s Retrying in %ds (%d/%d).',
                                 log_msg, retry_delay, attempt, retry_attempts)
                    time.sleep(retry_delay)
                    #continue

    """
    Cache available method names and create local methods for them.
    Makes tab completion work, for instance.
    """
    _cache_ttl = 30

    def __repr__(self):
        cls_name = self.__class__.__name__
        #rpc_name = self.__dict__.get('_remote_name', self._zerorpc_name(_cached=True))
        rpc_name = self._zerorpc_name(_cached=True)
        return "<%s '%s'>" % (cls_name, rpc_name)

    _zerorpc_builtin_methods = ['_zerorpc_list', '_zerorpc_name', '_zerorpc_ping',
                                '_zerorpc_help', '_zerorpc_args', '_zerorpc_inspect']

    def __dir__(self):
        return list(self._zerorpc_list(_cached=True)) + self._zerorpc_builtin_methods

    """
    Helpers for builtin ZeroRPC functions
    """

    def _zerorpc_name(self, *args, **kwargs):
        if not (kwargs.get('_cached') and self.__dict__.get('_remote_name')
                and self.__dict__.get('_remote_name_ts', 0) + self._cache_ttl > time.time()):
            self._remote_name = self(get_current_func_name(), *args, **kwargs)
            self._remote_name_ts = time.time()
        return self._remote_name

    def _zerorpc_list(self, *args, **kwargs):
        if not (kwargs.get('_cached') and self.__dict__.get('_remote_methods')
                and self.__dict__.get('_remote_methods_ts', 0) + self._cache_ttl > time.time()):
            self._remote_methods = self(get_current_func_name(), *args, **kwargs)
            self._remote_methods_ts = time.time()
        return self._remote_methods

    def _zerorpc_inspect(self, *args, **kwargs):
        ret = self(get_current_func_name(), *args, **kwargs)
        if ret:
            # Save remote name and methods
            self._remote_name = ret.get('name')
            self._remote_name_ts = time.time()
            self._remote_methods = ret.get('methods').keys()
            self._remote_methods_ts = time.time()
        return ret

    def _zerorpc_ping(self, *args, **kwargs):
        ret = self(get_current_func_name(), *args, **kwargs)
        if ret:
            try:
                self._remote_name = ret[1]
            except:
                pass
        return ret


class Client(ClientWithRetry):
    """Client with retry support and some defaults"""
    # FIXME c.close() does not seem to do anything as the timeout still happens.
    # As a temporary fix for dev'ing, it's now set to one hour.
    # This could become a problem if too many connections are opened...
    def __init__(self, connect=None, bind=None, heartbeat=1, timeout=3600, *args, **kwargs):
        super(Client, self).__init__(heartbeat=heartbeat, timeout=timeout, *args, **kwargs)
        if connect:
            self.connect(connect)
        elif bind:
            self.bind(bind)


class StorageClient(Client):
    def __init__(self, host):
        endpoint = 'tcp://%s:%d' % (host, 1785)
        super(StorageClient, self).__init__(connect=endpoint)


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
                    p('peer_ping', _retry_attempts=0)
                    logger.warning('Peer "%s" came back up!', p_host)
                except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
                    logger.error('Peer "%s" is still down: "%s"', p_host, e.message)
                    continue

            for pool in p.pools:
                try:
                    if not p('pool_is_healthy', pool):
                        logger.error('Pool "%s" is NOT healthy on "%s".', pool, p_host)
                    else:
                        logger.debug('Pool "%s" is healthy on "%s".', pool, p_host)
                except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
                    logger.error('Peer "%s" went down: "%s"', p_host, e.message)

                    # TODO rpyc and per peer filter
                    for res in local('drbd_res_list'):
                        local('drbd_primary', res)
                    local('target_scst_start')
                    # TODO HA IP lookup from Config, use signals to run this
                    # shit on down or up

        time.sleep(1)
