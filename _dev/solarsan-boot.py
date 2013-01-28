#!/usr/bin/env python

import logging
import time
import sh


def service(name, action='status'):
    return sh.service(name, action)


def cleanup_services():
    pass


class VolumeDrbdReplication(object):
    peer_port = None




def start_services():
    logging.info("Starting storage Pools..")
    service('zfs', 'start')
    time.sleep(30)

    logging.info("Starting volume replication")
    service('drbd', 'start')
    time.sleep(30)

    # Check DRBD status per resource, work with cluster peer to make sure all
    # primaries are on one host.
    for r in [0, 1, 2]:
        r = 'r%d' % r
        sh.drbdadm('primary', r)


def main():
    logging.info("SolarSan services will start in 15 minutes.")
    time.sleep(900)

    start_services()


if __name__ == '__main__':
    main()
