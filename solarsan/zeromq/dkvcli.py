
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from solarsan.pretty import pp
import random
import time
import pickle
# import zmq

try:
    from dkv import Dkv
except ImportError:
    from .dkv import Dkv

#SUBTREE = "/client/"
SUBTREE = ""


def get_client():
    # Create and connect dkv
    dkv = Dkv()
    #dkv.subtree = SUBTREE
    #dkv.subtree = '/client/'

    dkv.connect_via_discovery()

    #dkv.connect("tcp://san0", 5556)
    #dkv.connect("tcp://san1", 5556)

    return dkv


def test(dkv):
    dkv['trevorj_yup'] = 'fksdkfjksdf'
    dkv[SUBTREE + 'trevorj'] = 'woot'
    dkv[SUBTREE + 'trevorj-pickle'] = pickle.dumps(
        {'whoa': 'yeah', 'lbh': True})

    logger.debug('SHOW SERVER: %s', dkv.show('SERVER'))
    logger.debug('SHOW SERVERS: %s', dkv.show('SERVERS'))
    logger.debug('SHOW SEQ: %s', dkv.show('SEQ'))


def test_rand_cache(dkv):
    # Distribute as key-value message
    key = "%d" % random.randint(1, 10000)
    value = "%d" % random.randint(1, 1000000)
    dkv.set(key, value, random.randint(0, 30))


def main():
    dkv = get_client()
    #test(dkv)

    try:
        while True:
            #test_rand_cache(dkv)
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
