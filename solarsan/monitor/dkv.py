
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer

import zmq.utils.jsonapi as json
try:
    import cPickle as pickle
except ImportError:
    import pickle

# Temp hack
from solarsan.zeromq.clonecli import Clone
from solarsan.pretty import pp


"""
Distributed Key Value Store Manager
"""


class DkvGet(Event):
    """Gets a value"""


class DkvSet(Event):
    """Sets a value"""


#class DkvShow(Event):
#    """Shows a value"""


class DkvUpdate(Event):
    """Updates a value"""


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
        self.clone.signals.on_sub.connect(self._clone_on_sub)

    def _clone_on_sub(self, sender=None, key=None, value=None, **kwargs):
        #pp('%s=%s' % (key, value))
        self.fire(DkvUpdate({key: value, 'kvmsg': sender}))

    def dkv_get(self, key):
        return self.clone.get(key)

    def dkv_set(self, key, value, ttl=0):
        return self.clone.set(key, value, ttl=ttl)

    #def dkv_show(self, key):
    #    return self.clone.show(key)

    #def dkv_update(self, update, **kwargs):
    #    logger.debug('Got DKV update: %s kwargs=%s', update, kwargs)

    def started(self, component):
        self.test()

    def test(self):
        logger.debug('Testing DKV')
        clone = self.clone

        self.fire(DkvSet('/test/trevorj', 'woot'))

        clone['/test/trevorj_yup'] = 'fksdkfjksdf'
        clone['/test/trevorj2'] = 'woot'

        test_pickle = {'whoa': 'yeah', 'lbh': True}
        clone.set('/test/trevorj-pickle2', test_pickle, pickle=True)

        test_pickle_s = pickle.dumps(test_pickle)
        clone['/test/trevorj-pickle'] = test_pickle_s

        logger.debug('SHOW SERVER: %s', clone.show('SERVER'))
        logger.debug('SHOW SERVERS: %s', clone.show('SERVERS'))
        logger.debug('SHOW SEQ: %s', clone.show('SEQ'))

        logger.debug('Done Testing DKV')
