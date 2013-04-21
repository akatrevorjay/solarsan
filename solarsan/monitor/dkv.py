
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer
import pickle
# Temp hack
from _dev.zmq.clonecli import Clone
from solarsan.pretty import pp


"""
Distributed Key Value Store Manager
"""


class DkvGet(Event):
    """Gets a value in the distributed key value store"""


class DkvSet(Event):
    """Sets a value in the distributed key value store"""


class DkvShow(Event):
    """Shows a value in the distributed key value store"""


def get_dkv_client():
    clone = Clone()
    #clone.subtree = '/client/'
    clone.connect("tcp://localhost", 5556)
    clone.connect("tcp://localhost", 5566)
    return clone


class DkvManager(Component):
    channel = 'dkv'

    def __init__(self, channel=channel):
        super(DkvManager, self).__init__(channel=channel)
        self.clone = get_dkv_client()
        self.clone.on_sub.connect(self._on_dkv_get)

    def _on_dkv_get(self, sender=None, key=None, value=None):
        #pp('%s=%s' % (key, value))
        logger.debug('Got kv: %s=%s', key, value)
        #self.fire(DkvShow(key, value))

    def dkv_get(self, key):
        return self.clone.get(key)

    def dkv_show(self, key):
        return self.clone.show(key)

    def dkv_set(self, key, value, ttl=0):
        return self.clone.set(key, value, ttl=ttl)

    def started(self, component):
        self.test()

    def test(self):
        self.fire(DkvSet('/test/trevorj', 'woot'))

        clone = self.clone

        clone['/test/trevorj_yup'] = 'fksdkfjksdf'
        clone['/test/trevorj2'] = 'woot'
        clone['/test/trevorj-pickle'] = pickle.dumps({'whoa': 'yeah', 'lbh': True})

        logger.debug('SHOW SERVER: %s', clone.show('SERVER'))
        logger.debug('SHOW SERVERS: %s', clone.show('SERVERS'))
        logger.debug('SHOW SEQ: %s', clone.show('SEQ'))
