
from solarsan import logging, conf, LogMeta, LogMixin
logger = logging.getLogger(__name__)
#from solarsan.exceptions import NodeError

import weakref
import zmq.green as zmq
#import gevent

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


