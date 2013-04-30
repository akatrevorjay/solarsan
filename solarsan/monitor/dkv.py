
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from solarsan.pretty import pp
from circuits import Component, Event, Timer, handler
from solarsan.zeromq.dkvcli import get_client
from solarsan.cluster.models import Peer
from datetime import timedelta, datetime
import sys


"""
Distributed Key Value Store Manager
"""


class DkvGet(Event):
    """Gets a value"""


class DkvSet(Event):
    """Sets a value"""


class DkvShow(Event):
    """Shows a value"""


class DkvUpdate(Event):
    """Updates a value"""


class DkvWaitForConnected(Event):
    """Wait to be connected"""


class DkvNodeInfo(Event):
    """Marks local node as alive"""


class DkvManager(Component):
    channel = 'dkv'

    def __init__(self, channel=channel):
        super(DkvManager, self).__init__(channel=channel)
        self.dkv = get_client()
        self.dkv.signals.on_sub.connect(self._sub_cb)

        self.fire(DkvNodeInfo())
        Timer(10.0, DkvNodeInfo(), self.channel, persist=True).register(self)

        #DkvTest(self.dkv).register(self)

    @handler('dkv_wait_for_connected', channel='*')
    def dkv_wait_for_connected(self, timeout=None):
        self.dkv.wait_for_connected(timeout=timeout)

    def started(self, component):
        pass

    def _sub_cb(self, sender=None, key=None, value=None, **kwargs):
        self.fire(DkvUpdate({key: value, 'kvmsg': sender}))

    def dkv_get(self, key):
        return self.dkv.get(key)

    @handler('dkv_set', channel='*')
    def _on_dkv_set(self, key, value, ttl=0):
        return self.dkv.set(key, value, ttl=ttl)

    def dkv_show(self, key):
        return self.dkv.show(key)

    #def dkv_update(self, update, **kwargs):
    #    #pp('%s=%s' % (key, value))
    #    logger.debug('Got DKV update: %s kwargs=%s', update, kwargs)

    #def started(self, component):
    #    pass

    _local_peer = None

    def dkv_node_info(self):
        dkv = self.dkv

        if not self._local_peer:
            self._local_peer = Peer.get_local()
        local = self._local_peer

        node = '/nodes/%s' % (local.uuid)
        ttl = 20
        dkv.set('%s/alive' % node, 'yes', ttl=ttl)

        if not dkv.get('%s/hostname' % node, None):
            logger.info('Node info is not in Dkv; adding..')
            dkv.set('%s/hostname' % node, local.hostname)
            dkv.set('%s/cluster_iface' % node, local.cluster_iface)


class TestUpdate(Event):
    """Test Update"""


class TestUpdate2(TestUpdate):
    """Test Update 2"""


class DkvTest(Component):
    channel = 'dkv_test'

    def __init__(self, dkv, channel=channel):
        Component.__init__(self, channel=channel)
        self.dkv = dkv

        self.fire(TestUpdate())
        Timer(5.0, TestUpdate(), 'dkv_test', persist=True).register(self)
        ##Timer(10.0, TestUpdate2(), 'dkv_test', persist=True).register(self)

    #def started(self, component):
    #    self.test()

    def test_update(self):
        logger.debug('Testing DKV One')
        dkv = self.dkv

        node_base = '/nodes/%s' % conf.hostname

        dkv.set('%s.alive' % node_base, 'yes', ttl=10)

        #neighbors = Peer.objects.filter(last_seen__gt=datetime.now() - timedelta(days=1))
        ##dkv.set('%s.neighbors' % node_base, [p.hostname for p in neighbors], pickle=True)
        #dkv.set('%s.neighbors' % node_base, [p.hostname for p in neighbors], ttl=10)

    def test_update2(self):
        logger.debug('Testing DKV Two')
        dkv = self.dkv

        #dkv.set('/nodes/me', str(conf.hostname), ttl=10)
        dkv.set('/nodes2/%s.alive' % conf.hostname, 'yes', ttl=10)

    def test(self):
        logger.debug('Testing DKV')
        dkv = self.dkv

        self.fire(DkvSet('/test/trevorj', 'woot'))

        dkv['/test/trevorj_yup'] = 'fksdkfjksdf'
        dkv['/test/trevorj2'] = 'woot'

        test_pickle = {'whoa': 'yeah', 'lbh': True}
        dkv.set('/test/trevorj-pickle2', test_pickle, pickle=True)

        logger.debug('SHOW SERVER: %s', dkv.show('SERVER'))
        logger.debug('SHOW SERVERS: %s', dkv.show('SERVERS'))
        logger.debug('SHOW SEQ: %s', dkv.show('SEQ'))

        logger.debug('Done Testing DKV')
