#!/usr/bin/env python

from solarsan.zeromq.dkv.test_listener import *
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    #gevent.spawn_later(5, send_message, n)
    #gevent.spawn_later(15, send_message, n)
    #gevent.spawn_later(30, send_message, n)
    #gevent.spawn_later(60, send_message, n)

    while True:
        logger.info('top of loop')
        gevent.spawn(send_message, n)
        gevent.sleep(6)
