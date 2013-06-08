
from solarsan.zeromq.dkv.test import *
logger = logging.getLogger(__name__)


this_name = 'b'
peer_list = NODE_LIST.difference(set([this_name]))


n = get_node(this_name)
bind_node(n)
connect_nodes(n, *peer_list)
n.wait_until_ready()


if __name__ == '__main__':
    gevent.spawn_later(5, send_message, n)
    gevent.spawn_later(15, send_message, n)
    gevent.spawn_later(30, send_message, n)
    gevent.spawn_later(60, send_message, n)


    while True:
        logger.info('top of loop')
        gevent.sleep(10)
        gevent.spawn_later(10, send_message, n)
