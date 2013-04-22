
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer

import zmq.utils.jsonapi as json
try:
    import cPickle as pickle
except ImportError:
    import pickle

# Temp hack
from solarsan.zeromq.clonecli import Clone, get_client
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


class DkvManager(Component):
    channel = 'dkv'

    def __init__(self, channel=channel):
        super(DkvManager, self).__init__(channel=channel)
        self.clone = get_client()
        self.clone.signals.on_sub.connect(self._clone_on_sub)

        DkvTest(self.clone).register(self)

    def _clone_on_sub(self, sender=None, key=None, value=None, **kwargs):
        self.fire(DkvUpdate({key: value, 'kvmsg': sender}))

    def dkv_get(self, key):
        return self.clone.get(key)

    def dkv_set(self, key, value, ttl=0):
        return self.clone.set(key, value, ttl=ttl)

    #def dkv_show(self, key):
    #    return self.clone.show(key)

    #def dkv_update(self, update, **kwargs):
    #    #pp('%s=%s' % (key, value))
    #    logger.debug('Got DKV update: %s kwargs=%s', update, kwargs)

    #def started(self, component):
    #    pass


class TestUpdate(Event):
    """"""


class TestUpdate2(TestUpdate):
    """"""


class DkvTest(Component):
    channel = 'dkv'

    def __init__(self, clone, channel=channel):
        Component.__init__(self, channel=channel)
        self.clone = clone

        Timer(11.0, TestUpdate(), 'dkv', persist=True).register(self)
        Timer(15.0, TestUpdate2(), 'dkv', persist=True).register(self)

    #def started(self, component):
    #    self.test()

    def test_update(self):
        logger.debug('Testing DKV One')
        clone = self.clone

        clone.set('/nodes/%s/alive' % conf.hostname, 'yes', ttl=20)

        me = clone.get('me')
        if me != conf.hostname:
            clone.set('me', conf.hostname, ttl=20)

    #def test_update2(self):
    #    logger.debug('Testing DKV Two')
    #    clone = self.clone

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
