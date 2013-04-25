
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from solarsan.pretty import pp
from circuits import Component, Event, Timer
from solarsan.zeromq.dkvcli import get_client


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
        self.dkv = get_client()
        self.dkv.signals.on_sub.connect(self._dkv_on_sub)

        DkvTest(self.dkv).register(self)

    def _dkv_on_sub(self, sender=None, key=None, value=None, **kwargs):
        self.fire(DkvUpdate({key: value, 'kvmsg': sender}))

    def dkv_get(self, key):
        return self.dkv.get(key)

    def dkv_set(self, key, value, ttl=0):
        return self.dkv.set(key, value, ttl=ttl)

    #def dkv_show(self, key):
    #    return self.dkv.show(key)

    #def dkv_update(self, update, **kwargs):
    #    #pp('%s=%s' % (key, value))
    #    logger.debug('Got DKV update: %s kwargs=%s', update, kwargs)

    #def started(self, component):
    #    pass


class TestUpdate(Event):
    """Test Update"""


class TestUpdate2(TestUpdate):
    """Test Update 2"""


class DkvTest(Component):
    channel = 'dkv_test'

    def __init__(self, dkv, channel=channel):
        Component.__init__(self, channel=channel)
        self.dkv = dkv

        Timer(10.0, TestUpdate(), 'dkv_test', persist=True).register(self)
        Timer(10.0, TestUpdate2(), 'dkv_test', persist=True).register(self)

    #def started(self, component):
    #    self.test()

    def test_update(self):
        logger.debug('Testing DKV One')
        dkv = self.dkv

        dkv.set('/nodes/%s.neighbors' % conf.hostname, 'san0 san1')
        dkv.set('/nodes/%s.alive' % conf.hostname, 'yes', ttl=30)

    def test_update2(self):
        logger.debug('Testing DKV Two')
        dkv = self.dkv

        #dkv.set('/nodes/me', str(conf.hostname), ttl=30)
        dkv.set('/nodes2/%s.alive' % conf.hostname, 'yes', ttl=30)
        dkv.set('/nodes2/%s.neighbors' % conf.hostname, 'san0 san1')

    def test(self):
        logger.debug('Testing DKV')
        dkv = self.dkv

        self.fire(DkvSet('/test/trevorj', 'woot'))

        dkv['/test/trevorj_yup'] = 'fksdkfjksdf'
        dkv['/test/trevorj2'] = 'woot'

        test_pickle = {'whoa': 'yeah', 'lbh': True}
        dkv.set('/test/trevorj-pickle2', test_pickle, pickle=True)

        test_pickle_s = pickle.dumps(test_pickle)
        dkv['/test/trevorj-pickle'] = test_pickle_s

        logger.debug('SHOW SERVER: %s', dkv.show('SERVER'))
        logger.debug('SHOW SERVERS: %s', dkv.show('SERVERS'))
        logger.debug('SHOW SEQ: %s', dkv.show('SEQ'))

        logger.debug('Done Testing DKV')
