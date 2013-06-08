
from solarsan.zeromq.dkv.test import *
logger = logging.getLogger(__name__)


n = get_node('b')
bind_node(n)
connect_node(n, 'a')


while True:
    logger.info('top of loop')
    gevent.sleep(10)
