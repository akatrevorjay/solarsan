
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from solarsan.pretty import pp
import random
import time
import pickle
# import zmq

try:
    from clone import Clone
except ImportError:
    from .clone import Clone

#SUBTREE = "/client/"
SUBTREE = ""


def get_client():
    # Create and connect clone
    clone = Clone()
    #clone.subtree = SUBTREE
    #clone.subtree = '/client/'

    clone.connect_via_discovery()

    #clone.connect("tcp://san0", 5556)
    #clone.connect("tcp://san1", 5556)

    return clone


def test(clone):
    clone['trevorj_yup'] = 'fksdkfjksdf'
    clone[SUBTREE + 'trevorj'] = 'woot'
    clone[SUBTREE + 'trevorj-pickle'] = pickle.dumps(
        {'whoa': 'yeah', 'lbh': True})

    logger.debug('SHOW SERVER: %s', clone.show('SERVER'))
    logger.debug('SHOW SERVERS: %s', clone.show('SERVERS'))
    logger.debug('SHOW SEQ: %s', clone.show('SEQ'))


def test_rand_cache(clone):
    # Distribute as key-value message
    key = "%d" % random.randint(1, 10000)
    value = "%d" % random.randint(1, 1000000)
    clone.set(key, value, random.randint(0, 30))


def main():
    clone = get_client()
    #test(clone)

    try:
        while True:
            #test_rand_cache(clone)
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
