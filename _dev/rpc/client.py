#!/usr/bin/env python

import logging
import zerorpc
from solarsan.utils.stack import get_current_func_name


class ClientWithRetry(zerorpc.Client):
    """Wrapper of zerorpc.Client to automatically retry failed
    requests."""
    _retry_attempts = 0
    _retry_delay = 1

    def __init__(self, *args, **kwargs):
        for i in ['retry_attempts', 'retry_delay']:
            if i in kwargs:
                setattr(self, '_%s' % i, kwargs.pop(i))
        zerorpc.Client.__init__(self, *args, **kwargs)

    def __call__(self, method, *args, **kwargs):
        retry_attempts = kwargs.pop('_retry_attempts', self._retry_attempts)
        retry_delay = kwargs.pop('_retry_delay', self._retry_delay)
        for attempt in xrange(-1, retry_attempts):
            try:
                ret = zerorpc.Client.__call__(self, method, *args, **kwargs)
                return ret
            except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
                log_msg = 'Connection error: "%s".' % e
                if attempt >= retry_attempts:
                    # raise exception on last attempt
                    logging.error('%s Retry count exeeded (%d/%d), giving up.',
                                  log_msg, attempt, retry_attempts)
                    raise e
                else:
                    # try again a little later
                    logging.warning('%s Retrying in %ds (%d/%d).',
                                    log_msg, retry_delay, attempt + 2, retry_attempts + 1)
                    time.sleep(retry_delay)
                    continue

    """
    Cache available method names and create local methods for them.
    Makes tab completion work, for instance.
    """

    def __repr__(self):
        cls_name = self.__class__.__name__
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
        if not (kwargs.get('_cached') and self.__dict__.get('_remote_name')):
            self._remote_name = self(get_current_func_name(), *args, **kwargs)
        return self._remote_name

    def _zerorpc_list(self, *args, **kwargs):
        if not (kwargs.get('_cached') and self.__dict__.get('_remote_methods')):
            self._remote_methods = self(get_current_func_name(), *args, **kwargs)
        return self._remote_methods

    def _zerorpc_inspect(self, *args, **kwargs):
        ret = self(get_current_func_name(), *args, **kwargs)
        if ret:
            # Save remote name and methods
            self._remote_name = ret.get('name')
            self._remote_methods = ret.get('methods').keys()
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


def get_client(conn):
    c = Client()
    #c.connect('tcp://%s:%d' % (host, settings.CLUSTER_RPC_PORT))
    c.connect(conn)
    return c


import time


class Target(object):
    wwn = None
    volumes = None


resources = ['dpool/%s' % i for i in ['r0', 'r1', 'r2']]


def become_target_primary(target):
    pass


def storage_pool_health_loop():
    c = get_client('ipc://sock/storage')

    pool = 'dpool'

    lost_count = 0
    while True:
        try:
            if not c.pool_is_healthy(pool):
                logging.error('Pool is NOT healthy')

            lost_count = 0
        except zerorpc.LostRemote:
            logging.error('Lost remote!')
            lost_count += 1

        if lost_count > 5:
            logging.error('Remote is gone!')

        time.sleep(1)
