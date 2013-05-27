
from solarsan import logging, conf, LogMeta, LogMixin
logger = logging.getLogger(__name__)
# from solarsan.exceptions import NodeError

from .encoder import EJSONEncoder

# from .message import MessageContainer
# from .channel import Channel

import gevent
import zmq.green as zmq
from uuid import uuid4
# from datetime import datetime
# from functools import partial
# import weakref

from reflex.base import Reactor, Binding, Ruleset
from reflex.control import Binding, Callable, Binding, Event, EventManager, \
    PackageBattery, ReactorBattery, RulesetBattery

import xworkflows


from . import managers as node_managers

from .managers.heartbeat import HeartbeatManager
from .managers.sequence import SequenceManager
from .managers.transaction import TransactionManager
from .managers.debugger import DebuggerManager
from .managers.keyvalue import KeyValueNode, KeyValueStorageNode


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
    _default_managers = [HeartbeatManager, SequenceManager, TransactionManager]
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

        # peers
        self.peers = dict()

        # sockets
        self.rtr = None
        self.pub = None
        self.sub = None
        self._socks = dict()

        # event handlers
        self.handlers = dict()

        # 0MQ init
        if not hasattr(self, 'ctx'):
            self._ctx = zmq.Context.instance()
        if not hasattr(self, 'poller'):
            self._poller = zmq.Poller()

        """ Managers """

        self.managers = dict()

        managers = kwargs.get('managers')
        if not managers:
            managers = []
        managers += self._default_managers

        #self.battery = ReactorBattery()

        for cls in managers:
            self.log.debug('Loading manager %s', cls)
            cls(self)
            #self.battery.load_objects(
            #    self.events, node_managers, 'Manager', self)

    """ State """

    state = NodeState()

    @property
    def is_ready(self):
        return self.state.is_ready

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
            #self.log.debug('Add channel: %s', channel_name)
            self.handlers[channel_name] = list()

        if handler not in self.handlers[channel_name]:
            # handler = weakref.proxy(handler)
            self.log.debug('Add handler: %s chan=%s', handler, channel_name)
            self.handlers[channel_name].append(handler)

    def remove_handler(self, channel_name, handler):
        if channel_name in self.handlers:
            if handler in self.handlers[channel_name]:
                self.log.debug(
                    'Remove handler: %s chan=%s', handler, channel_name)
                self.handlers[channel_name].remove(handler)
            if not self.handlers[channel_name]:
                #self.log.debug('Remove channel: %s', channel_name)
                self.handlers.pop(channel_name)

    """ Run """

    def _start_managers(self, *managers):
        if not managers:
            managers = self.managers.iteritems()
        for k, v in managers:
            if not getattr(v, 'started', None):
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
            gevent.sleep(0.1)

    def _add_sock(self, sock, cb, flags=zmq.POLLIN):
        self._poller.register(sock, flags)
        self._socks[sock] = (cb, flags)

    def _zmq_init(self):
        if not self.rtr:
            rtr = self.rtr = self._ctx.socket(zmq.ROUTER)
            rtr.setsockopt(zmq.IDENTITY, self.uuid)
            self._add_sock(rtr, self._on_rtr_received)

    @xworkflows.transition()
    def bind(self, rtr_addr, pub_addr):
        self._zmq_init()
        ctx = self._ctx
        rtr = self.rtr

        if self.pub:
            self.pub.close()
        pub = self.pub = ctx.socket(zmq.PUB)

        for sock in (rtr, pub):
            sock.linger = 0

        rtr.bind(rtr_addr)
        pub.bind(pub_addr)

        gevent.sleep(0.1)

    def connect_peer(self, peer):
        self.connect(peer.uuid,
                     'tcp://%s:%s' % (peer.cluster_addr, conf.ports.dkv_rtr),
                     'tcp://%s:%s' % (peer.cluster_addr, conf.ports.dkv_pub),
                     )

    @xworkflows.transition()
    def connect(self, uuid, rtr_addr, pub_addr):
        """Connects to peer"""
        self._zmq_init()
        ctx = self._ctx

        # If we already have uuid
        if uuid in self.peers:
            self.peers[uuid].sub.close()

        # Connect router
        self.rtr.connect(rtr_addr)

        # Connect subscriber
        sub = ctx.socket(zmq.SUB)
        for sock in (sub, ):
            sock.linger = 0
        sub.setsockopt(zmq.SUBSCRIBE, b'')
        sub.connect(pub_addr)

        # Add subscriber socket to our sock repertoire
        self._add_sock(sub, self._on_sub_received)

        # Add peer object
        self.peers[uuid] = dict(sub=sub, rtr_addr=rtr_addr, pub_addr=pub_addr)

        # Set timeout
        # t = gevent.Timeout(seconds=30)

        # Greet peer
        # >> If timeout hits here, try again <<
        # Upon greet received, connect to pub_addr specified

        gevent.sleep(0.1)

    def shutdown(self):
        for s, sv in self._socks.iteritems():
            s.close()
        self._socks = dict()

    def broadcast(self, channel_name, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = ['solarsan', str(channel_name)]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.pub.send_multipart(l)

    def unicast(self, to_uuid, channel_name, message_type, *parts):
        if to_uuid == self.uuid:
            self.dispatch(
                self.uuid, channel_name, message_type, parts)
            return
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = [str(to_uuid), str(channel_name)]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.rtr.send_multipart(l)

    def _dispatch(self, from_uuid, channel_name, message_type, parts):
        # self.log.debug('Dispatch: from=%s chan=%s msg_type=%s parts=%s',
        # from_uuid, channel_name, message_type, parts)

        # Handle wildcard channel
        if channel_name != '*':
            self._dispatch(from_uuid, '*', message_type, parts)

        handlers = self.handlers.get(channel_name, None)
        if handlers:
            for h in handlers:
                f = getattr(h, 'receive_' + message_type, None)
                if f:
                    # self.log.debug('Dispatching to: %s', h)
                    # f(from_uuid, *parts)
                    gevent.spawn(f, from_uuid, *parts)
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
