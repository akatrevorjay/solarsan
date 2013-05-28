
from solarsan.zeromq.dkv.test import *
logger = logging.getLogger(__name__)


n = get_node('a')
bind_node(n)
connect_node(n, 'b')


#gevent.spawn_later(5, send_message, n)
#gevent.spawn_later(15, send_message, n)
#gevent.spawn_later(30, send_message, n)
#gevent.spawn_later(60, send_message, n)

gevent.sleep(1)

while True:
    #logger.info('top of loop')
    gevent.spawn(send_message, n)
    gevent.sleep(6)
