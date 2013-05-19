
import zmq
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback, DelayedCallback
from zmq.eventloop.zmqstream import ZMQStream
from .channel import Channel, NodeChannel
from .encoder import SimpleEncoder, JSONEncoder


class Node(object):
    '''
    Messages are handled by adding instances to the message_handlers list. The
    first instance that contains a method named 'receive_<message_type>'
    will have that method called. The first argument is always the message
    sender's uuid. The remaining positional arguments are filled with the
    parts of the ZeroMQ message.
    '''
    #_default_encoder = SimpleEncoder
    _default_encoder = JSONEncoder

    def __init__(self, uuid, encoder=_default_encoder()):
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

    def add_handler(self, channel_name, handler):
        if not channel_name in self.message_handlers:
            self.message_handlers[channel_name] = list()
        self.message_handlers[channel_name].append(handler)

    def _zmq_init(self):
        if not hasattr(self, 'ctx'):
            self.ctx = zmq.Context.instance()
        ctx = self.ctx
        if not hasattr(self, 'loop'):
            self.loop = IOLoop.instance()
        loop = self.loop

    def _bind(self, rtr_addr, pub_addr):
        self._zmq_init()

        if self.rtr:
            self.rtr.close()
        rtr = ctx.socket(zmq.ROUTER)

        if self.pub:
            self.pub.close()
        pub = ctx.socket(zmq.PUB)

        for sock in (rtr, pub):
            sock.linger = 0

        rtr.setsockopt(zmq.IDENTITY, self.uuid)
        rtr.bind(rtr_addr)

        pub.bind(pub_addr)

    def connect(self, peer_uuid, peer_rtr_addr):
        '''Connects to peer
        '''
        self._zmq_init()

        rtr_addr = peer_rtr_addr

        self.peers[peer_uuid] = dict(sub=None, rtr_addr=rtr_addr)

        if self.sub:
            self.sub.close()
        sub = self.ctx.socket(zmq.SUB)
        for sock in (sub, ):
            sock.linger = 0
        sub.setsockopt(zmq.SUBSCRIBE, 'test')


    def connect_many(self, **peers):
        '''Connects to peer(s), specified as dict of uuid => (zmq_rtr_addr, zmq_pub_addr)
        '''

        #for peer_uuid, peer in self.peers.iteritems():
        #    sub.connect(peers

        #for uuid, tpl in peers.iteritems():
        #    self.sub.connect(tpl[1])
        #    if self.uuid < uuid:
        #        # We only need 1 connection between any two router nodes so
        #        # we'll make it the responsibility of the lower uuid node to
        #        # initiate the connection
        #        self.rtr.connect(tpl[0])

        self.chan_rtr = NodeChannel(rtr, rtr)
        self.chan_rtr.on_receive(self._on_rtr_received)
        self.chan_pubsub = NodeChannel(sub, pub)
        self.chan_pubsub.on_receive(self._on_sub_received)

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
        l = ['zpax', channel_name]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.pub.send(l)

    def unicast(self, to_uuid, channel_name, message_type, *parts):
        if to_uuid == self.uuid:
            self.dispatch(
                self.uuid, channel_name, message_type, parts)
            return
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = [str(to_uuid), channel_name]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.rtr.send(l)

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
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)
