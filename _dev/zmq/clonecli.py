
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.pretty import pp
import random
import time
import pickle
#import zmq
from clone import Clone

SUBTREE = "/client/"
#SUBTREE = ""


def main():
    # Create and connect clone
    clone = Clone()
    clone.subtree = SUBTREE
    clone.connect("tcp://localhost", 5556)
    clone.connect("tcp://localhost", 5566)

    clone['trevorj_yup'] = 'fksdkfjksdf'
    clone[SUBTREE + 'trevorj'] = 'woot'
    clone[SUBTREE + 'trevorj-pickle'] = pickle.dumps({'whoa': 'yeah', 'lbh': True})

    logger.debug('SHOW SERVER: %s', clone.show('SERVER'))
    logger.debug('SHOW SERVERS: %s', clone.show('SERVERS'))
    logger.debug('SHOW SEQ: %s', clone.show('SEQ'))

    try:
        while True:
            ## Distribute as key-value message
            #key = "%d" % random.randint(1,10000)
            #value = "%d" % random.randint(1,1000000)
            #clone.set(key, value, random.randint(0,30))
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
