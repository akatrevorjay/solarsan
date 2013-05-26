
from solarsan import logging, conf, LogMeta
logger = logging.getLogger(__name__)
# from solarsan.exceptions import NodeError

from .encoder import EJSONEncoder
from .managers import HeartbeatSequenceManager, \
    HeartbeatManager, SequenceManager, TransactionManager, DebuggerManager
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



class Node(gevent.Greenlet, Reactor):

    '''
    Messages are handled by adding instances to the handlers list. The
    first instance that contains a method named 'receive_<message_type>'
    will have that method called. The first argument is always the message
    sender's uuid. The remaining positional arguments are filled with the
    parts of the ZeroMQ message.
    '''

    __metaclass__ = LogMeta
    debug = False

    uuid = None

    peers = None
    _socks = None

    _default_managers = [HeartbeatManager, SequenceManager, TransactionManager]
    if debug:
        _default_managers += [DebuggerManager]

    _evm = None
    _handlers = None

    _ctx = None
    _poller = None

    _encoder_cls = EJSONEncoder
    _encoder = None
    _managers = None

    def __init__(self, uuid=None, encoder=_encoder_cls()):
        gevent.Greenlet.__init__(self)
        evm = self.evm = EventManager()
        Reactor.__init__(self, evm, uuid=uuid, encoder=encoder)

    def init(self, *args, **kwargs):
        # uuid
        uuid = kwargs.get('uuid')
        if not uuid:
            uuid = uuid4().get_hex()
        self.uuid = str(uuid)

        # encoder
        encoder = kwargs.get('encoder')
        self._encoder = encoder

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

        # load managers
        self._init_managers(kwargs.get('managers'))

    def _init_managers(self, managers=None):
        self._managers = dict()

        if not managers:
            managers = []
        managers += self._default_managers

        self.battery = ReactorBattery()

        for cls in managers:
            self.log.debug('Loading manager %s', cls)
            cls(self)
            #self.battery.load_objects(
            #    self.evm, managers, 'Manager', self)

    def add_manager(self, manager, name=None):
        if not name:
            # name = repr(manager)
            name = manager.__class__.__name__
        if not name in self._managers:
            self._managers[name] = manager
        if self.started:
            self._run_managers()

    def remove_manager(self, manager, name=None):
        if not name:
            # name = repr(manager)
            name = manager.__class__.__name__
        if name in self._managers:
            del self._managers[name]

    def __repr__(self):
        return "<%s uuid='%s'>" % (self.__class__.__name__, self.uuid)

    def add_handler(self, channel_name, handler):
        if not channel_name in self.handlers:
            self.log.debug('Add channel: %s', channel_name)
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
                self.log.debug('Remove channel: %s', channel_name)
                self.handlers.pop(channel_name)

    def _add_sock(self, sock, cb, flags=zmq.POLLIN):
        self._poller.register(sock, flags)
        self._socks[sock] = (cb, flags)

    def _run_managers(self):
        for k, v in self._managers.iteritems():
            if not getattr(v, 'started', None):
                self.log.debug('Spawning Manager %s: %s', k, v)
                v.link(self.remove_manager)
                v.start()

    def _run(self):
        self.running = True
        self._run_managers()

        while self.running:
            socks = dict(self._poller.poll(timeout=0))
            if socks:
                # self.log.debug('socks=%s', socks)
                for s, sv in self._socks.iteritems():
                    cb, flags = sv
                    if s in socks and socks[s] == flags:
                        cb(s.recv_multipart())
            gevent.sleep(0.1)

    def _zmq_init(self):
        if not self.rtr:
            rtr = self.rtr = self._ctx.socket(zmq.ROUTER)
            rtr.setsockopt(zmq.IDENTITY, self.uuid)
            self._add_sock(rtr, self._on_rtr_received)

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

    def connect(self, uuid, rtr_addr, pub_addr):
        """Connects to peer"""
        self._zmq_init()
        ctx = self._ctx

        # If we already have uuid
        if uuid in self.peers:
            self.peers[uuid].sub.close()

        self.rtr.connect(rtr_addr)

        sub = ctx.socket(zmq.SUB)
        for sock in (sub, ):
            sock.linger = 0
        sub.setsockopt(zmq.SUBSCRIBE, b'')
        sub.connect(pub_addr)

        self._add_sock(sub, self._on_sub_received)

        # Peer
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
        l.extend(self._encoder.encode(self.uuid, message_type, parts))
        self.pub.send_multipart(l)

    def unicast(self, to_uuid, channel_name, message_type, *parts):
        if to_uuid == self.uuid:
            self.dispatch(
                self.uuid, channel_name, message_type, parts)
            return
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = [str(to_uuid), str(channel_name)]
        l.extend(self._encoder.encode(self.uuid, message_type, parts))
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
        from_uuid, message_type, parts = self._encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        # self.log.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self._encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    # def receive_discovery(self, peer_uuid, peer_router):
    #    """Connects to discovered peer"""
    #    self.connect(peer_uuid, router=peer_router)



