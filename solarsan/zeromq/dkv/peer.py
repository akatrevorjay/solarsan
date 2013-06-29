
from solarsan import logging, conf, LogMixin
logger = logging.getLogger(__name__)

import gevent
import gevent.coros
import gevent.event

import weakref
import xworkflows
from reflex.base import Reactor
from reflex.data import Event


class Peer(xworkflows.WorkflowEnabled, Reactor, LogMixin):

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

            ('dead', 'Dead state'),
        )
        transitions = (
            ('start', 'init', 'connecting'),
            ('connect', ('init', 'connecting'), 'connecting'),
            ('_mark_connected', 'connecting', 'greeting'),
            # TODO don't send during syncing as we've already greeted
            ('receive_greet', ('syncing', 'greeting'), 'syncing'),
            ('_on_synced', 'syncing', 'ready'),

            ('shutdown', [x[0] for x in states], 'dead')
        )

    state = State()

    debug = False

    cluster_addr = None

    def __init__(self, uuid):
        self.uuid = str(uuid)
        self.is_local = False
        self.connected = False
        self.dead = False

    def __repr__(self):
        meta = dict(
            uuid=str(self.uuid),
            # cluster_addr=self.cluster_addr,
            rtr_port=self.rtr_port,
            pub_port=self.pub_port,
            # connected=self.connected,
            # is_local=self.is_local,
            state=self.state,
        )

        parts = ['%s=%s' % (k, v) for k, v in meta.iteritems()]
        return "<%s %s>" % (self.__class__.__name__, ', '.join(parts))

    def _debug(self, *args, **kwargs):
        if self.debug:
            return self.log.debug(*args, **kwargs)

    """ Connection """

    @xworkflows.transition()
    def connect(self, node):
        self.log.debug('Connecting to peer: %s', self)

        Reactor.__init__(self, node.events)

        self._node = weakref.proxy(node)

        self.bind(self._on_node_syncing, 'node_syncing')
        self.bind(self._on_node_ready, 'node_ready')

        self.bind(self._on_peer_synced, 'peer_synced')
        self.bind(self._on_peer_connection_rtr, 'peer_connection_rtr')

        gevent.spawn(self._node.add_peer, self)

    def _on_node_syncing(self, event, node):
        self.log.debug('Node is syncing!')

    def _on_node_ready(self, event, node):
        self.log.debug('Node is ready!')

    _connected_sub = None
    _connected_rtr = None

    # Tests SUB
    def receive_beat(self, meta):
        """ Heartbeat """
        self._debug('Got beat')
        if not self.connected:
            self._node.wait_until_syncing()
            self._connected_sub = True
            gevent.spawn(self.check_if_connected)

    # Tests RTR
    def _on_peer_connection_rtr(self, event, peer):
        if peer != self:
            return
        if not self.connected:
            self._node.wait_until_syncing()
            self._connected_rtr = True
            gevent.spawn(self.check_if_connected)

    def check_if_connected(self):
        if self._connected_rtr and self._connected_sub:
            self._mark_connected()

    @xworkflows.transition()
    def _mark_connected(self):
        self.log.info('Connected to %s', self)
        self.connected = True

    @xworkflows.after_transition('_mark_connected')
    def _after_connected(self, r):
        self.trigger(Event('peer_connected'), self)

    @xworkflows.transition()
    def receive_greet(self):
        if self.state.is_syncing:
            self.log.warning(
                'Received greet from %s while already syncing', self)
            raise xworkflows.AbortTransition
        self.log.debug('Received greet from %s', self)

    @xworkflows.on_enter_state('syncing')
    def _on_enter_syncing(self, r):
        self.trigger(Event('peer_syncing'), self)

    @xworkflows.on_enter_state('ready')
    def _on_ready(self, *args):
        self.trigger(Event('peer_ready'), self)

    @xworkflows.transition()
    def _on_synced(self):
        self.log.debug('Completed syncing %s', self)

    def _on_peer_synced(self, event, peer):
        if peer != self:
            return
        gevent.spawn_later(1, self._on_synced)

    @xworkflows.transition()
    def shutdown(self):
        self.log.debug('Shutting down peer: %s', self)

        self._disconnect()

        self.dead = True

        if hasattr(self, '_node'):
            self._node.remove_peer(self)

    def _disconnect(self):
        # self.log.debug('Disconnecting from peer: %s', self)
        self.connected = False
        self._connected_rtr = False
        self._connected_sub = False

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


#class PeerContainer(_BaseDict):
#
#    def __init__(self, node, *args, **kwargs):
#        self._node = node
#        _BaseDict.__init__(self, *args, **kwargs)
#
#    def add(self, peer):
#        self[peer.uuid] = peer
#
#    __append__ = add
#
#    def remove(self, peer_or_uuid):
#        if isinstance(peer, basestring):
#            uuid = peer
#        else:
#            uuid = str(peer.uuid)
#        del peer
#        self.pop(uuid, None)
#
#
#class Peers(_BaseDict):
#
#    def __init__(self, node):
#        self._node = node
#
#        self.connected = PeerContainer(node)
#        #self.ready = PeerContainer(node)
