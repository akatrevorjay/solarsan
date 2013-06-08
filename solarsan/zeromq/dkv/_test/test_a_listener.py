
from solarsan.zeromq.dkv.test import *
logger = logging.getLogger(__name__)

import sys
this_name = sys.argv[1]
peer_list = NODE_LIST.difference(set([this_name]))


n = get_node(this_name)
bind_node(n)
connect_nodes(n, *peer_list)
n.wait_until_ready()


if __name__ == '__main__':
    while True:
        logger.info('top of loop')
        gevent.sleep(10)
