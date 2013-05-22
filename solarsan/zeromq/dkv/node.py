
from solarsan import logging, conf
logger = logging.getLogger(__name__)
#from solarsan.exceptions import NodeError

from .encoder import EJSONEncoder
from .managers import HeartbeatSequenceManager, TransactionManager, DebuggerManager
#from .message import MessageContainer
#from .channel import Channel

import gevent
import zmq.green as zmq
from uuid import uuid4
#from datetime import datetime
#from functools import partial
#import weakref


class Node(gevent.Greenlet):
    '''
    Messages are handled by adding instances to the message_handlers list. The
    first instance that contains a method named 'receive_<message_type>'
    will have that method called. The first argument is always the message
    sender's uuid. The remaining positional arguments are filled with the
    parts of the ZeroMQ message.
    '''

    _default_encoder = EJSONEncoder
    _default_managers = (HeartbeatSequenceManager, TransactionManager)
    _default_managers += (DebuggerManager, )
    uuid = None

    sequence = 0
    _pending_sequence = sequence

    _store = dict()


    @property
    def pending_sequence(self):
        if self.sequence < self._pending_sequence:
            return self._pending_sequence
        else:
            return self.sequence

    @pending_sequence.setter
    def pending_sequence(self, value):
        self._pending_sequence = value


    def __repr__(self):
        return "<%s uuid='%s'>" % (self.__class__.__name__, self.uuid)

    def __init__(self, uuid=None, encoder=_default_encoder()):
        gevent.Greenlet.__init__(self)

        if not uuid:
            uuid = uuid4().get_hex()
        self.uuid = uuid
        self.encoder = encoder

        # Sockets
        self.rtr = None
        self.pub = None
        self.sub = None

        # State
        self.running = False
        self.sequence = 0

        # Dictionary of uuid -> (rtr_addr, pub_addr)
        self.peers = dict()

        # Dictionary of channel_name => list( message_handlers )
        self.message_handlers = dict()
        #self.message_handlers = weakref.WeakValueDictionary()

        # Managers
        self.managers = dict()
        #self.managers = weakref.WeakValueDictionary()

        # Sockets
        self._socks = dict()

        if not hasattr(self, 'ctx'):
            self.ctx = zmq.Context.instance()

        if not hasattr(self, 'poller'):
            self.poller = zmq.Poller()

        for cls in self._default_managers:
            cls(self)

    def add_manager(self, manager, name=None):
        if not name:
            #name = repr(manager)
            name = manager.__class__.__name__
        if not name in self.managers:
            self.managers[name] = manager
        if self.started:
            self._run_managers()

    def remove_manager(self, manager, name=None):
        if not name:
            #name = repr(manager)
            name = manager.__class__.__name__
        if name in self.managers:
            del self.managers[name]

    def add_handler(self, channel_name, handler):
        if not channel_name in self.message_handlers:
            logger.debug('Add channel: %s', channel_name)
            self.message_handlers[channel_name] = list()

        if handler not in self.message_handlers[channel_name]:
            #handler = weakref.proxy(handler)
            logger.debug('Add handler: %s chan=%s', handler, channel_name)
            self.message_handlers[channel_name].append(handler)

    def remove_handler(self, channel_name, handler):
        if channel_name in self.message_handlers:
            if handler in self.message_handlers[channel_name]:
                logger.debug('Remove handler: %s chan=%s', handler, channel_name)
                self.message_handlers[channel_name].remove(handler)
            if not self.message_handlers[channel_name]:
                logger.debug('Remove channel: %s', channel_name)
                self.message_handlers.pop(channel_name)

    def _add_sock(self, sock, cb, flags=zmq.POLLIN):
        self.poller.register(sock, flags)
        self._socks[sock] = (cb, flags)

    def _run_managers(self):
        for k, v in self.managers.iteritems():
            if not getattr(v, 'started', None):
                logger.debug('Spawning Manager %s: %s', k, v)
                v.link(self.remove_manager)
                v.start()

    def _run(self):
        self.running = True
        self._run_managers()

        while self.running:
            socks = dict(self.poller.poll(timeout=0))
            if socks:
                #logger.debug('socks=%s', socks)
                for s, sv in self._socks.iteritems():
                    cb, flags = sv
                    if s in socks and socks[s] == flags:
                        cb(s.recv_multipart())
            gevent.sleep(0.1)

    def _zmq_init(self):
        if not self.rtr:
            rtr = self.rtr = self.ctx.socket(zmq.ROUTER)
            rtr.setsockopt(zmq.IDENTITY, self.uuid)
            self._add_sock(rtr, self._on_rtr_received)

    def bind(self, rtr_addr, pub_addr):
        self._zmq_init()
        ctx = self.ctx
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
        ctx = self.ctx

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
        #t = gevent.Timeout(seconds=30)

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
        #logger.debug('Dispatch: from=%s chan=%s msg_type=%s parts=%s', from_uuid, channel_name, message_type, parts)

        # Handle wildcard channel
        if channel_name != '*':
            self._dispatch(from_uuid, '*', message_type, parts)

        handlers = self.message_handlers.get(channel_name, None)
        if handlers:
            for h in handlers:
                f = getattr(h, 'receive_' + message_type, None)
                if f:
                    #logger.debug('Dispatching to: %s', h)
                    #f(from_uuid, *parts)
                    gevent.spawn(f, from_uuid, *parts)
                    #break

    def _on_rtr_received(self, raw_parts):
        # discard source address. We'll use the one embedded in the message
        # for consistency
        #logger.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        #logger.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    #def receive_discovery(self, peer_uuid, peer_router):
    #    """Connects to discovered peer"""
    #    self.connect(peer_uuid, router=peer_router)
