#!/usr/bin/env python

import logging
logger = logging.getLogger('rpc.client')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(module)s %(message)s @%(funcName)s:%(lineno)d')
#formatter = logging.Formatter('%(name)s.%(module)s/%(processName)s[%(process)d]: [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d')
ch.formatter = formatter
logger.addHandler(ch)


import zerorpc
from solarsan.utils.stack import get_current_func_name
from solarsan.utils.cache import cached_property


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
        for attempt in xrange(0, retry_attempts+1):
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


import time


class Target(object):
    wwn = None
    volumes = None


resources = ['dpool/%s' % i for i in ['r0', 'r1', 'r2']]


def become_target_primary(target):
    pass


from socket import gethostname


STATES = {
    0: 'ONLINE',
    1: 'OFFLINE',
    #5: 'DEAD',
}


class Host(object):
    hostname = None
    cluster_addr = None

    is_local = None

    state = None

    def __init__(self, hostname, cluster_addr):
        self.hostname = hostname
        self.cluster_addr = cluster_addr

        for k, v in STATES.iteritems():
            setattr(self, v, k)

        self.is_local = gethostname() == hostname
        self.state = self.ONLINE
        self._lost_count = 0

    @property
    def storage(self):
        if not hasattr(self, '_storage'):
            self._storage = StorageClient(self.cluster_addr)
        return self._storage

    def __call__(self, method, *args, **kwargs):
        default_on_timeout = kwargs.pop('_default_on_timeout', 'exception')

        try:
            ret = self.storage(method, *args, **kwargs)

            if self.state != self.ONLINE:
                logger.error('Peer "%s" went ONLINE!', self.hostname)
                self.state = self.ONLINE
                self._lost_count = 0

            return ret
        except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
            self._lost_count += 1

            #if self._lost_count < 5 and self.state != self.OFFLINE:
            if self.state != self.OFFLINE:
                logger.error('Peer "%s" went OFFLINE!', self.hostname)
                self.state = self.OFFLINE
            #elif self.state != self.DEAD:
            #    logger.error('Peer "%s" went DEAD!', self.hostname)
            #    self.state = self.DEAD
            #else:
            #    logger.error('Peer "%s" is still %s', self.hostname, self.state)

            if default_on_timeout == 'exception':
                raise e
            else:
                return default_on_timeout

    #def __getattr__(self, method):
    #    return zerorpc.Client.__getattr__(self, method)

    @cached_property(ttl=300)
    def pools(self):
        if not getattr(self, '_%s' % get_current_func_name(), None):
            self._pools = self('pool_list', _default_on_timeout={})
        return self._pools

    @cached_property(ttl=300)
    def cvols(self):
        if not getattr(self, '_%s' % get_current_func_name(), None):
            self._cvols = self('cluster_volume_list', _default_on_timeout=[])
        return self._cvols


def storage_pool_health_loop():
    hostname = gethostname()
    from cluster.models import Peer

    peers = Peer.objects.all()
    clients = {}
    local = None
    for peer in peers:
        p_hostname = peer.hostname
        p_cluster_addr = peer.cluster_addr

        logger.info('Connecting to Peer "%s" via "%s".', p_hostname, p_cluster_addr)
        clients[p_hostname] = Host(p_hostname, p_cluster_addr)

        if clients[p_hostname].is_local:
            if local:
                raise Exception('Found two Peers with my hostname!')
            local = clients[hostname]

    #for p_host, p in clients.iteritems():
    #    logger.info('Peer "%s" pools found: "%s".', p_host, p.pools.keys())
    #    # TODO get hostname of peer for each replicated volume
    #    logger.info('Peer "%s" replicated volumes found: "%s".', p_host, p.cvols)

    while True:
        for p_host, p in clients.iteritems():
            print
            logger.debug('Peer "%s"' % p_host)

            if p.state == p.OFFLINE:
                try:
                    p('peer_ping', _retry_attempts=0)
                    logger.error('Peer "%s" came back up!', p_host)
                except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
                    logger.warning('Peer "%s" is still down.', p_host)
                    continue

            for pool in p.pools:
                try:
                    if not p('pool_is_healthy', pool):
                        logger.error('Pool "%s" is NOT healthy on "%s".', pool, p_host)
                    #else:
                    #    logger.debug('Pool "%s" is healthy on "%s".', pool, p_host)
                except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
                    #logger.error('Peer "%s" comms lost: "%s"', p_host, e.message)
                    pass

        time.sleep(1)
