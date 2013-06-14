
from solarsan import logging, conf, LogMixin
logger = logging.getLogger(__name__)
# from solarsan.exceptions import NodeError

import gevent
import gevent.coros
import gevent.event

import zmq.green as zmq
import weakref
import xworkflows
from reflex.base import Reactor
from reflex.data import Event


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
        meta = dict(
            uuid=str(self.uuid),
            #cluster_addr=self.cluster_addr,
            rtr_port=self.rtr_port,
            pub_port=self.pub_port,
            #connected=self.connected,
            #is_local=self.is_local,
            state=self.state,
        )

        parts = ['%s=%s' % (k, v) for k, v in meta.iteritems()]
        return "<%s %s>" % (self.__class__.__name__, ', '.join(parts))

    """ Connection """

    def _connect_sub(self):
        # Connect subscriber
        self.sub.connect(self.pub_addr)

    @xworkflows.transition()
    def connect(self, node):
        # if self.debug:
        self.log.debug('Connecting to peer: %s', self)

        self._node = weakref.proxy(node)

        Reactor.__init__(self, node.events)
        self.bind(self._on_node_syncing, 'node_syncing')
        self.bind(self._on_node_ready, 'node_ready')

        #if hasattr(self, 'sub'):
        #    delattr(self, 'sub')
        self.sub = sub = self._node._ctx.socket(zmq.SUB)
        for sock in (sub, ):
            sock.linger = 0
        sub.setsockopt(zmq.SUBSCRIBE, b'')

        self._connect_sub()

        self._node.add_peer(self)

    def _on_node_syncing(self, event, node):
        self.log.debug('Node is syncing!')

    def _on_node_ready(self, event, node):
        self.log.debug('Node is ready!')

    def receive_beat(self, meta):
        """ Heartbeat """
        #if self.debug:
        #    self.log.debug('Got beat')
        if not self.connected:
            self._node.wait_until_syncing()
            # gevent.sleep(0.1)
            gevent.spawn(self._connected)
            #self._connected()

    @xworkflows.transition()
    def _connected(self):
        self.log.info('Connected to %s', self)
        self.connected = True

        # TODO event on connect, use that to start sending greets

        # HACKERY
        self.greet()

    def greet(self, is_reply=False):
        return self._node.greeter.greet(self, is_reply)

    @xworkflows.transition()
    def receive_greet(self):
        self.log.debug('Received greet from %s', self)
        gevent.spawn(self._sync)

    def _sync(self):
        self.log.debug('Syncing %s', self)
        # TODO Sync
        gevent.spawn_later(2, self._synced)

    @xworkflows.on_enter_state('ready')
    def _on_ready(self, *args):
        self.trigger(Event('peer_ready'), self)

    @xworkflows.transition()
    def _synced(self):
        self.log.debug('Completed syncing %s', self)

    @xworkflows.transition()
    def shutdown(self):
        self.log.debug('Shutting down peer: %s', self)

        self._disconnect()
        if hasattr(self, '_node'):
            self._node.remove_peer(self)

    def _disconnect(self):
        #self.log.debug('Disconnecting from peer: %s', self)
        #if hasattr(self, 'sub'):
        #    # This is not supported by ZMQ. God speed.
        #    # self.sub.close()
        #    # Deleting the attribute will help it get GCd, which automatically
        #    # closes it.
        #    delattr(self, 'sub')
        pass

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
