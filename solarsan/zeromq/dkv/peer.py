
from solarsan import logging, conf, LogMeta, LogMixin
logger = logging.getLogger(__name__)
#from solarsan.exceptions import NodeError

import gevent
import zmq.green as zmq
import weakref
import xworkflows
from reflex.base import Reactor


class Peer(LogMixin, Reactor, xworkflows.WorkflowEnabled):
    uuid = None
    cluster_addr = None
    is_local = None

    connected = None

    class State(xworkflows.Workflow):
        initial_state = 'init'
        states = (
            ('init',            'Initial state'),
            ('connecting',      'Connecting state'),
            ('greeting',        'Greeting state'),
            ('ready',           'Ready state'),

            # TODO event system for peers, maybe peer managers per se?
            # that way we can avoid shit like this, relying on a node plugin
            # to handle part of our state, just a simple, init() ready() would
            # suffice IMO
            # TODO with above plugin or event system, this should become a state
            # relying upon status of all of them, more generic terms, could even
            # use 'greeted' or some shit.
            ('syncing',         'Sync state'),
        )
        transitions = (
            ('start', 'init', 'connecting'),
            ('connect', ('init', 'connecting'), 'connecting'),
            ('_connected', 'connecting', 'greeting'),
            ('receive_greet', 'greeting', 'syncing'),
            ('_synced', 'syncing', 'ready'),

            ('shutdown', [x[0] for x in states], 'init')
        )

    state = State()

    def __init__(self, uuid):
        self.uuid = uuid
        self.is_local = False
        self.connected = False

    def __repr__(self):
        return "<%s uuid='%s'>" % (self.__class__.__name__, str(self.uuid))

    """ Connection """

    def _connect_sub(self):
        # Connect subscriber
        self.sub.connect(self.pub_addr)

    @xworkflows.transition()
    def connect(self, node):
        #if self.debug:
        self.log.debug('Connecting to peer: %s', self)

        self._node = weakref.proxy(node)

        #Reactor.__init__(self, node.events)
        #self.bind(self._on_node_ready, 'node_ready')

        self.sub = sub = self._node._ctx.socket(zmq.SUB)
        for sock in (sub, ):
            sock.linger = 0
        sub.setsockopt(zmq.SUBSCRIBE, b'')

        self._connect_sub()

        self._node.add_peer(self)

    #def _on_node_ready(self, event, node):
    #    self.log.debug('Node is ready!')

    def receive_beat(self, meta):
        if not self.connected:
            self._node.wait_until_ready()
            self._connected()

    @xworkflows.transition()
    def _connected(self):
        self.log.info('Connected to %s', self)
        self.connected = True
        gevent.spawn(self.greet)

    def greet(self):
        self.log.debug('Greeting %s', self)
        # TODO actually greet, remove this hackery
        gevent.spawn(self.receive_greet)

    @xworkflows.transition()
    def receive_greet(self):
        self.log.debug('Received greet from %s', self)
        gevent.spawn(self._sync)

    def _sync(self):
        self.log.debug('Syncing %s', self)
        # TODO Sync
        gevent.spawn_later(2, self._synced)

    @xworkflows.transition()
    def _synced(self):
        self.log.debug('Completed syncing %s', self)

    @xworkflows.transition()
    def shutdown(self):
        self.log.debug('Shutting down peer: %s', self)

        if hasattr(self, '_node'):
            self._node.remove_peer(self)
        self._disconnect()

    def _disconnect(self):
        self.log.debug('Disconnecting from peer: %s', self)
        if hasattr(self, 'sub'):
            self.sub.close()

    """ Helpers """

    rtr_port = conf.ports.dkv_rtr

    @property
    def rtr_addr(self):
        return 'tcp://%s:%s' % (self.cluster_addr, self.rtr_port)

    pub_port = conf.ports.dkv_pub

    @property
    def pub_addr(self):
        return 'tcp://%s:%s' % (self.cluster_addr, self.pub_port)

    """ Messaging """

    def unicast(self, channel, message_type, *parts, **kwargs):
        return self._node.unicast(self, channel, message_type, *parts, **kwargs)

    """ Heartbeat """

    #def receive_beat(self, meta):
    #    pass


