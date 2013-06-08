
from test_b import *
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    while True:
        logger.info('top of loop')
        gevent.sleep(10)
