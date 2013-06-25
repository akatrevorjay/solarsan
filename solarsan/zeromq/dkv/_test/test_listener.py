#!/usr/bin/env python

from solarsan.zeromq.dkv.test import *
logger = logging.getLogger(__name__)

import os
import sys

this_name = None
if len(sys.argv) > 1:
    this_name = sys.argv[1]
else:
    names = sys.argv[0].split('_')
    for name in names:
        if name in NODE_LIST:
            this_name = name
if not this_name:
    raise Exception("Could not decide on a name for myself!")


peer_list = NODE_LIST.difference(set([this_name]))

logger.info('this_name=%s; peer_list=%s', this_name, peer_list)

n = get_node(this_name)
bind_node(n)
#connect_nodes(n, *peer_list)
n.wait_until_ready()


if __name__ == '__main__':
    while True:
        logger.info('top of loop')
        gevent.sleep(10)
