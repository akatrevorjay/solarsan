
from solarsan import logging, conf
logger = logging.getLogger(__name__)

import gevent
import zmq.green as zmq

#from .channel import Channel, NodeChannel
from .encoder import UJSONEncoder, JSONEncoder
from uuid import uuid4

'''
Gevent



'''


class Node(object):
    '''
    Messages are handled by adding instances to the message_handlers list. The
    first instance that contains a method named 'receive_<message_type>'
    will have that method called. The first argument is always the message
    sender's uuid. The remaining positional arguments are filled with the
    parts of the ZeroMQ message.
    '''
    _default_encoder = UJSONEncoder
    #_default_encoder = JSONEncoder

    def __init__(self, uuid=None, encoder=_default_encoder()):
        if not uuid:
            uuid = uuid4().get_hex()
        self.uuid = uuid
        self.encoder = encoder

        # Sockets
        self.rtr = None
        self.pub = None
        self.sub = None

        # Dictionary of uuid -> (rtr_addr, pub_addr)
        self.peers = dict()

        # Dictionary of channel_name => list( message_handlers )
        self.message_handlers = dict()

        if not hasattr(self, 'ctx'):
            self.ctx = zmq.Context.instance()
        if not hasattr(self, 'poller'):
            self.poller = zmq.Poller()
        self._socks = dict()

    def add_handler(self, channel_name, handler):
        if not channel_name in self.message_handlers:
            self.message_handlers[channel_name] = list()
        if handler not in self.message_handlers[channel_name]:
            self.message_handlers[channel_name].append(handler)

    def _add_sock(self, sock, cb, flags=zmq.POLLIN):
        self.poller.register(sock, flags)
        self._socks[sock] = (cb, flags)

    def _loop(self):
        while True:
            socks = dict(self.poller.poll(timeout=0))
            if socks:
                #logger.debug('socks=%s', socks)
                for s, sv in self._socks.iteritems():
                    cb, flags = sv
                    if s in socks and socks[s] == flags:
                        cb(s.recv_multipart())
            gevent.sleep(0.1)

    def start(self):
        if not hasattr(self, '_greenlet'):
            self._greenlet = gevent.spawn(self._loop)
            #self._greenlet.link(self._loop_exit)
            #self._greenlet.link_exception(self._loop_exception)

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

    def shutdown(self):
        if self.rtr:
            self.rtr.close()
            self.rtr = None
        if self.pub:
            self.pub.close()
            self.pub = None
        if self.sub:
            self.sub.close()
            self.sub = None

    def broadcast(self, channel_name, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = ['solarsan', channel_name]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.pub.send_multipart(l)

    def unicast(self, to_uuid, channel_name, message_type, *parts):
        if to_uuid == self.uuid:
            self.dispatch(
                self.uuid, channel_name, message_type, parts)
            return
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = [str(to_uuid), channel_name]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.rtr.send_multipart(l)

    def _dispatch(self, from_uuid, channel_name, message_type, parts):
        handlers = self.message_handlers.get(channel_name, None)
        if handlers:
            for h in handlers:
                f = getattr(h, 'receive_' + message_type, None)
                if f:
                    f(from_uuid, *parts)
                    break

    def _on_rtr_received(self, raw_parts):
        # discard source address. We'll use the one embedded in the message
        # for consistency
        logger.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        logger.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    #def receive_discovery(self, peer_uuid, peer_router):
    #    """Connects to discovered peer"""
    #    self.connect(peer_uuid, router=peer_router)
