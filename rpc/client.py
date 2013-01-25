#!/usr/bin/env python

import logging

#from django.conf import settings

import zerorpc


def get_client(conn):
    # FIXME c.close() does not seem to do anything as the timeout still happens.
    # As a temporary fix for dev'ing, it's now set to one hour.
    # This could become a problem if too many connections are opened...
    c = zerorpc.Client(heartbeat=1, timeout=3600)
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

