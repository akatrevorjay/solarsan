#!/usr/bin/env python

import logging
import zerorpc
import functools
from solarsan.utils.stack import get_current_func_name
#from solarsan.utils.funcs import func_to_method
#from datetime import datetime, timedelta


class ClientWithRetry(zerorpc.Client):
    """Wrapper of zerorpc.Client to automatically retry failed
    requests."""
    _retry_attempts = 0
    _retry_delay = 1

    def __init__(self, *args, **kwargs):
        for i in ['retry_attempts', 'retry_delay']:
            if i in kwargs:
                setattr(self, '_%s' % i, kwargs.pop(i))
        #super(ClientWithRetry, self).__init__(*args, **kwargs)
        zerorpc.Client.__init__(self, *args, **kwargs)

        self._inspect_cache = {}
        #self._filled_methods = []
        #self._zerorpc_inspect_fill()

    def __call__(self, method, *args, **kwargs):
        retry_attempts = kwargs.pop('_retry_attempts', self._retry_attempts)
        retry_delay = kwargs.pop('_retry_delay', self._retry_delay)
        for attempt in xrange(-1, retry_attempts):
            try:
                ret = zerorpc.Client.__call__(self, method, *args, **kwargs)
                #ret = super(ClientWithRetry, self).__call__(method, *args, **kwargs)
                return ret
            except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
                log_msg = 'Timeout during RPC: "%s".'
                if attempt >= retry_attempts:
                    # raise exception on last attempt
                    logging.error('%s Retry count exeeded (%d/%d), giving up.',
                                  log_msg, e.name, attempt, retry_attempts)
                    raise
                else:
                    # try again a little later
                    logging.warning('%s Retrying in %ds (%d/%d).',
                                    log_msg, e.name, retry_delay, attempt, retry_attempts)
                    time.sleep(retry_delay)
                    continue

    """
    Cache available method names and create local methods for them.
    Makes tab completion work, for instance.
    """

    @property
    def _zerorpc_cache(self):
        if not self._inspect_cache:
            try:
                self._inspect_cache = self._zerorpc_inspect(retry_attempts=0)
            except (zerorpc.TimeoutExpired, zerorpc.LostRemote):
                pass
        return self._inspect_cache

    '''
    def _zerorpc_fill_method(self, method):
        # TODO arguments, doc
        setattr(self, method, functools.partial(self.__call__, method))
        self._filled_methods.append(method)

    def _zerorpc_inspect_fill(self):
        methods = self._zerorpc_cache['methods']
        filled_methods = self._filled_methods

        # Remove filled methods that no longer exist, just in case
        for i, k in enumerate(filled_methods):
            if k in methods:
                continue
            logging.warning('Method "%s" has disappeared; Removing.', k)
            delattr(self, 'filled_methods')
            del filled_methods[i]

        # Fill methods
        for k in methods.iterkeys():
            if k not in filled_methods:
                self._zerorpc_fill_method(k)
    '''

    def __repr__(self):
        cls_name = self.__class__.__name__
        rpc_name = self._zerorpc_cache.get('name', 'Unknown')
        return "<%s '%s'>" % (cls_name, rpc_name)

    def __dir__(self):
        methods = self._zerorpc_cache.get('methods', {}).keys()
        if not methods:
            methods = self._zerorpc_list(_retry_attempts=0)
        return methods

    #def __getattr__(self, method):
    #    if not method.isalnum():

    """
    Helpers for builtin ZeroRPC functions
    """

    def _zerorpc_list(self, *args, **kwargs):
        return self(get_current_func_name(), *args, **kwargs)

    def _zerorpc_inspect(self, *args, **kwargs):
        return self(get_current_func_name(), *args, **kwargs)

    def _zerorpc_ping(self, *args, **kwargs):
        return self(get_current_func_name(), *args, **kwargs)


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
