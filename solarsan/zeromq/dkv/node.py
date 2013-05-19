
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

    def __init__(self, node_uuid, encoder=_default_encoder()):
        self.uuid = node_uuid
        self.encoder = encoder

        # Dictionary of uuid -> (rtr_addr, pub_addr)
        self.peers = None
        # Dictionary of channel_name => list( message_handlers )
        self.message_handlers = dict()

        self.rtr = None
        self.pub = None
        self.sub = None

    def add_message_handler(self, channel_name, handler):
        if not channel_name in self.message_handlers:
            self.message_handlers[channel_name] = list()
        self.message_handlers[channel_name].append(handler)

    def connect(self, peers):
        '''
        peers - Dictionary of uuid => (zmq_rtr_addr, zmq_pub_addr)
        '''
        if not self.uuid in peers:
            raise Exception('Missing local node configuration')

        self.peers = peers

        if not hasattr(self, 'ctx'):
            self.ctx = zmq.Context.instance()
        ctx = self.ctx
        if not hasattr(self, 'loop'):
            self.loop = ioloop.IOLoop.instance()
        loop = self.loop

        if self.rtr:
            self.rtr.close()
        if self.pub:
            self.pub.close()
        if self.sub:
            self.sub.close()

        rtr = ctx.socket(zmq.ROUTER)
        pub = ctx.socket(zmq.PUB)
        sub = ctx.socket(zmq.SUB)

        for sock in (rtr, pub, sub):
            sock.linger = 0
        rtr.setsockopt(zmq.IDENTITY, uuid)
        sub.setsockopt(zmq.SUBSCRIBE, 'test')

        rtr.bind(peers[self.uuid][0])
        pub.bind(peers[self.uuid][1])
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
        self.rtr.close()
        self.pub.close()
        self.sub.close()
        self.rtr = None
        self.pub = None
        self.sub = None

    def broadcast_message(self, channel_name, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = ['zpax', channel_name]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.pub.send(l)

    def unicast_message(self, to_uuid, channel_name, message_type, *parts):
        if to_uuid == self.uuid:
            self.dispatch_message(
                self.uuid, channel_name, message_type, parts)
            return
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = [str(to_uuid), channel_name]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.rtr.send(l)

    def _dispatch_message(self, from_uuid, channel_name, message_type, parts):
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
        self._dispatch_message(from_uuid, channel_name, message_type, parts)

    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch_message(from_uuid, channel_name, message_type, parts)
