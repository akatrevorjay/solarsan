
from solarsan import logging, conf, LogMeta, LogMixin
logger = logging.getLogger(__name__)
from solarsan.exceptions import NodeError, PeerUnknown

from .encoder import EJSONEncoder

# from .message import MessageContainer
# from .channel import Channel

import gevent
import zmq.green as zmq
from uuid import uuid4
# from datetime import datetime
# from functools import partial
import weakref

from reflex.base import Reactor, Binding, Ruleset
from reflex.control import Callable, Event, EventManager, \
    PackageBattery, ReactorBattery, RulesetBattery

import xworkflows

from . import managers as node_managers

from .managers.heartbeat import HeartbeatManager
from .managers.sequence import SequenceManager
from .managers.transaction import TransactionManager
from .managers.debugger import DebuggerManager
from .managers.keyvalue import KeyValueManager

#from .base import _BaseDict


class Peer(LogMixin):
    uuid = None
    cluster_addr = None
    is_local = None

    def __init__(self, uuid):
        self.uuid = uuid
        self.is_local = False

    """ Connection """

    def _connect_sub(self):
        # Connect subscriber
        self.sub.connect(self.pub_addr)

    def connect(self, node):
        #if self.debug:
        self.log.debug('Connecting to peer: %s', self)

        self._node = weakref.proxy(node)

        self.sub = sub = self._node._ctx.socket(zmq.SUB)
        for sock in (sub, ):
            sock.linger = 0
        sub.setsockopt(zmq.SUBSCRIBE, b'')

        self._connect_sub()

        self._node.add_peer(self)

    def shutdown(self):
        #if self.debug:
        self.log.debug('Shutting down peer: %s', self)

        if hasattr(self, '_node'):
            self._node.remove_peer(self)
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


class NodeState(xworkflows.Workflow):
    initial_state = 'init'
    states = (
        ('init',            'Initial state'),
        ('starting',        'Starting state'),
        ('connecting',      'Connecting state'),
        ('greeting',        'Greeting state'),
        ('syncing',         'Sync state'),
        ('ready',           'Ready state'),
    )
    transitions = (
        ('start', 'init', 'starting'),
        ('bind', ('init', 'starting'), 'connecting'),
        ('connect', 'connecting', 'connecting'),
        ('connected', 'connecting', 'greeting'),
        ('greeted', 'greeting', 'syncing'),
        ('synced', 'syncing', 'ready'),
    )


class Node(LogMixin, gevent.Greenlet, Reactor, xworkflows.WorkflowEnabled):

    '''
    Messages are handled by adding instances to the handlers list. The
    first instance that contains a method named 'receive_<message_type>'
    will have that method called. The first argument is always the message
    sender's uuid. The remaining positional arguments are filled with the
    parts of the ZeroMQ message.
    '''

    debug = False

    _default_encoder_cls = EJSONEncoder
    _default_managers = [HeartbeatManager, SequenceManager, TransactionManager, KeyValueManager]
    if debug:
        _default_managers += [DebuggerManager]

    uuid = None
    peers = None
    _socks = None
    _ctx = None
    _poller = None

    def __init__(self, uuid=None, encoder=_default_encoder_cls()):
        gevent.Greenlet.__init__(self)
        self.events = EventManager()
        Reactor.__init__(self, self.events, uuid=uuid, encoder=encoder)

    def init(self, *args, **kwargs):
        # uuid
        uuid = kwargs.get('uuid')
        if not uuid:
            uuid = uuid4().get_hex()
        self.uuid = str(uuid)

        # encoder
        self.encoder = kwargs['encoder']

        # event handlers
        self.handlers = dict()

        # peers
        self.peers = dict()

        """ Sockets """

        # 0MQ init
        if not self._ctx:
            self._ctx = zmq.Context.instance()
        if not self._poller:
            self._poller = zmq.Poller()

        # dict of sockets to poll
        self._socks = dict()

        # router socket
        rtr = self.rtr = self._ctx.socket(zmq.ROUTER)
        rtr.setsockopt(zmq.IDENTITY, self.uuid)
        self._add_sock(rtr, self._on_rtr_received)

        # publisher socket
        self.pub = self._ctx.socket(zmq.PUB)

        for sock in (self.rtr, self.pub):
            sock.linger = 0

        """ Managers """

        self.managers = dict()

        managers = kwargs.get('managers')
        if not managers:
            managers = []
        managers += self._default_managers

        #self.battery = ReactorBattery()

        for cls in managers:
            if self.debug:
                self.log.debug('Loading manager %s', cls)
            cls(self)
            #self.battery.load_objects(
            #    self.events, node_managers, 'Manager', self)

    """ State """

    state = NodeState()

    @property
    def is_ready(self):
        # TODO HACK
        #return self.state.is_ready
        return True

    active = is_ready

    """ Managers """

    def add_manager(self, manager, name=None):
        if not name:
            # name = repr(manager)
            name = manager.__class__.__name__
            name = name.rsplit('Manager', 1)[0]

        if not name in self.managers:
            self.managers[name] = manager
            if self.started:
                self._start_managers(manager)

    def remove_manager(self, manager, name=None):
        if not name:
            name = manager.__class__.__name__
            name = name.rsplit('Manager', 1)[0]

        if name in self.managers:
            del self.managers[name]

    """ Handlers """

    def add_handler(self, channel_name, handler):
        if not channel_name in self.handlers:
            if self.debug:
                self.log.debug('Add channel: %s', channel_name)
            self.handlers[channel_name] = list()

        if handler not in self.handlers[channel_name]:
            # handler = weakref.proxy(handler)
            if self.debug:
                self.log.debug('Add handler: %s chan=%s', handler, channel_name)
            self.handlers[channel_name].append(handler)

    def remove_handler(self, channel_name, handler):
        if channel_name in self.handlers:
            if handler in self.handlers[channel_name]:
                if self.debug:
                    self.log.debug(
                        'Remove handler: %s chan=%s', handler, channel_name)
                self.handlers[channel_name].remove(handler)
            if not self.handlers[channel_name]:
                if self.debug:
                    self.log.debug('Remove channel: %s', channel_name)
                self.handlers.pop(channel_name)

    """ Run """

    def _start_managers(self, *managers):
        if not managers:
            managers = self.managers.iteritems()
        for k, v in managers:
            if not getattr(v, 'started', None):
                #if self.debug:
                self.log.debug('Spawning Manager %s: %s', k, v)
                v.link(self.remove_manager)
                v.start()
        gevent.sleep(0)

    @xworkflows.transition()
    def start(self):
        #self.bind()
        return gevent.Greenlet.start(self)

    def _run(self):
        self.running = True
        self._start_managers()

        while self.running:
            socks = dict(self._poller.poll(timeout=0))
            if socks:
                # self.log.debug('socks=%s', socks)
                for s, sv in self._socks.iteritems():
                    cb, flags = sv
                    if s in socks and socks[s] == flags:
                        cb(s.recv_multipart())
            gevent.sleep(0)

    def _add_sock(self, sock, cb, flags=zmq.POLLIN):
        self._poller.register(sock, flags)
        self._socks[sock] = (cb, flags)

    @xworkflows.transition()
    def bind(self, rtr_addr, pub_addr):
        self.rtr.bind(rtr_addr)
        self.pub.bind(pub_addr)

        gevent.sleep(0.1)

    @xworkflows.transition()
    def connect(self):
        self.log.info('Connecting..')

        peers = self.peers.copy()
        gs = []
        for uuid, peer in peers.iteritems():
            g = gevent.spawn(self.connect_peer, peer)
            gs.append(g)
        for g in gs:
            g.join(timeout=10.0)

    def add_peer(self, peer):
        uuid = str(peer.uuid)

        # TODO I don't believe this is the correct way of handling this.
        # We should just ensure the connection is still alive.
        # Then again, this is really just a corner case.
        #
        # If we already have uuid, replace it
        if peer.uuid in self.peers:
            self.peers[uuid].shutdown()

        if self.debug:
            self.log.debug('Adding peer: %s', peer)
        self.peers[uuid] = peer
        self.connect_peer(peer)

    def remove_peer(self, peer):
        self.log.info('Removing peer: %s', peer)
        uuid = str(peer.uuid)
        if uuid in self.peers:
            del self.peers[uuid]

    def connect_peer(self, peer):
        """Connects to peer"""

        self.log.info('Connecting to peer: %s', peer)

        # Connect router
        self.rtr.connect(peer.rtr_addr)

        # Add subscriber socket to our sock repertoire
        self._add_sock(peer.sub, self._on_sub_received)

        # Set timeout
        # t = gevent.Timeout(seconds=30)

        # Greet peer
        # >> If timeout hits here, try again <<
        # Upon greet received, connect to pub_addr specified

        gevent.sleep(0.1)

    def shutdown(self):
        self.log.info('Shuttind down Node: %s', self)

        # peer sub socks
        for s, sv in self._socks.iteritems():
            s.close()

        # extraneous socks
        for attr in ('pub', ):
            s = getattr(self, attr, None)
            if s is not None:
                s.close()

    def broadcast(self, channel_name, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = ['solarsan', str(channel_name)]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.pub.send_multipart(l)

    def get_peer(self, peer_or_uuid, exception=PeerUnknown):
        peer = None
        if not isinstance(peer_or_uuid, Peer):
            peer = self.peers.get(peer_or_uuid)
        else:
            peer = peer_or_uuid
        if not peer and exception:
            raise exception(peer_or_uuid)
        return peer

    def unicast(self, peer, channel_name, message_type, *parts):
        peer = self.get_peer(peer)

        if peer.uuid == self.uuid:
            self.dispatch(
                self.uuid, channel_name, message_type, parts)
            return

        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]

        l = [str(peer.uuid), str(channel_name)]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))

        self.rtr.send_multipart(l)

    def _dispatch(self, from_peer, channel_name, message_type, parts, _looped=None):
        peer = self.get_peer(from_peer, exception=None)
        if peer is None:
            self.log.error('Ignoring dispatch call with an unknown peer: %s', from_peer)
            return

        if self.debug and not _looped:
            self.log.debug('Dispatch: peer=%s chan=%s msg_type=%s parts=%s',
                           peer, channel_name, message_type, parts)

        # Handle wildcard channel
        if channel_name != '*':
            self._dispatch(peer, '*', message_type, parts, _looped=True)

        handlers = self.handlers.get(channel_name, None)
        if handlers:
            for h in handlers:
                f = getattr(h, 'receive_' + message_type, None)
                if f:
                    #if self.debug:
                    #self.log.debug('Dispatching to: %s', h)
                    gevent.spawn(f, peer, *parts)
                    # break

    def _on_rtr_received(self, raw_parts):
        # discard source address. We'll use the one embedded in the message
        # for consistency
        # self.log.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        # self.log.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    # def receive_discovery(self, peer_uuid, peer_router):
    #    """Connects to discovered peer"""
    #    self.connect(peer_uuid, router=peer_router)

    def __repr__(self):
        return "<%s uuid='%s'>" % (self.__class__.__name__, self.uuid)
