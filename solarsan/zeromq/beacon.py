
from solarsan import logging, conf
log = logging.getLogger(__name__)
from solarsan.utils.stack import get_last_func_name

import time
import errno
from functools import partial

import zmq
#from zmq import ZMQError
from zmq.eventloop.ioloop import IOLoop, DelayedCallback, PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

import socket
import uuid
import struct

beaconv1 = struct.Struct('3sB16sH')
beaconv2 = struct.Struct('3sB16sHBB4s')

T_TO_I = {
    #'udp': 1,
    'tcp': 1,
    'pgm': 2,
}

I_TO_T = {v: k for k, v in T_TO_I.items()}

NULL_IP = '\x00' * 4


class Peer:
    def __init__(self, id, uuid, socket, addr, time_=None, beacon=None):
        self.id = id
        self.uuid = uuid
        self.socket = socket
        self.addr = addr
        self.time = time_ or time.time()

        self.transport, host = addr.split('://', 1)
        self.host, port = host.rsplit(':', 1)
        #self.host = self.addr.rsplit(':', 1)[0].split('://', 1)[1]

    #@property
    #def endpoint(self):
    #    return '%s://%s:%d' % (self.transport, self.host, self.port)


class Beacon(object):
    """ZRE beacon emmiter.  http://rfc.zeromq.org/spec:20

    This implements only the base UDP beaconing 0mq socket
    interconnection layer, and disconnected peer detection.
    """
    ctx = None

    _debug = False
    #_debug = True

    service_port = None

    _peer_cls = Peer

    def __init__(self,
                 broadcast_addr='',
                 #broadcast_port=35713,
                 broadcast_port=conf.ports.discovery,
                 service_addr='*',
                 service_transport='tcp',
                 service_socket_type=zmq.ROUTER,
                 beacon_interval=1,
                 dead_interval=10,
                 on_recv_msg=None,
                 on_peer_connected=None,
                 on_peer_lost=None,
                 send_beacon=True):

        self.broadcast_addr = broadcast_addr
        self.broadcast_port = broadcast_port
        self.service_addr = service_addr
        self.service_transport = service_transport
        self.service_socket_type = service_socket_type
        self.beacon_interval = beacon_interval
        self.dead_interval = dead_interval

        self.on_recv_msg_cb = on_recv_msg
        self.on_peer_connected_cb = on_peer_connected
        self.on_peer_lost_cb = on_peer_lost

        self.send_beacon = send_beacon

        self.peers = {}
        if service_addr != '*':
            self.service_addr_bytes = socket.inet_aton(
                socket.gethostbyname(service_addr))
        else:
            self.service_addr_bytes = NULL_IP

        self.me = uuid.uuid4().bytes

        self._peer_init_kwargs = {}
        #self._peer_init_kwargs['beacon'] = self

    def init(self):
        if not self.ctx:
            self.ctx = zmq.Context.instance()

        self.loop = IOLoop.instance()

        self.router = self.ctx.socket(self.service_socket_type)
        endpoint = '%s://%s' % (self.service_transport, self.service_addr)
        self.service_port = self.router.bind_to_random_port(endpoint)

        self.router = ZMQStream(self.router, self.loop)
        self.router.on_recv(self._recv_router)
        #self.router.set_close_callback(self._peer_socket_closed)

        self.broadcaster = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP)
        self.broadcaster.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_BROADCAST,
            2)
        self.broadcaster.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1)

        self.broadcaster.setblocking(0)
        self.broadcaster.bind((self.broadcast_addr, self.broadcast_port))

        self.loop.add_handler(self.broadcaster.fileno(), self._recv_beacon, self.loop.READ)

        if self.send_beacon:
            self._send_beacon_cb = PeriodicCallback(
                self._send_beacon, self.beacon_interval * 1000, self.loop)

    def start(self, loop=True):
        self.init()
        if self.send_beacon:
            self._send_beacon_cb.start()
        if loop:
            return self.loop.start()

    def _recv_beacon(self, fd, events):
        """Greenlet that received udp beacons
        """
        while True:
            try:
                data, (peer_addr, port) = self.broadcaster.recvfrom(beaconv2.size)
            except socket.error as e:
                if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                    log.exception('Error recving beacon:', e)
                    raise e
                return

            try:
                if len(data) == beaconv1.size:
                    greet, ver, peer_id, peer_port = beaconv2.unpack(data)
                    #greet, ver, peer_id, peer_port = beaconv1.unpack(data)
                    peer_transport = 1
                    peer_socket_type = zmq.ROUTER
                    peer_socket_address = NULL_IP
                    if ver != 1:
                        continue
                else:
                    greet, ver, peer_id, peer_port, \
                        peer_transport, peer_socket_type, \
                        peer_socket_address = beaconv2.unpack(data)
                    if ver != 2:
                        continue
            except Exception:
                continue

            if greet != 'ZRE':
                continue

            if peer_id == self.me:
                continue

            if peer_socket_address != NULL_IP:
                peer_addr = socket.inet_ntoa(peer_socket_address)

            peer_transport = I_TO_T[peer_transport]

            self.handle_beacon(peer_id, peer_transport, peer_addr,
                               peer_port, peer_socket_type)

    def _send_beacon(self):
        """Greenlet that sends udp beacons at intervals.
        """
        beacon = beaconv2.pack(
            'ZRE', 2, self.me,
            self.service_port,
            T_TO_I[self.service_transport],
            self.service_socket_type,
            self.service_addr_bytes)

        try:
            self.broadcaster.sendto(
                beacon,
                ('<broadcast>', self.broadcast_port))
        except socket.error as e:
            if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                log.exception('Error sending beacon:', e)
                raise e
            return

        # check for losts
        now = time.time()
        for peer_id in self.peers.keys():
            peer = self.peers.get(peer_id)
            if not peer:
                continue

            if now - peer.time > self.dead_interval:
                log.debug('Lost peer %s.', peer.uuid)

                peer.socket.close()
                self._on_peer_lost(peer)

                del self.peers[peer_id]

    def _recv_peer(self, msg):
        """Greenlet that receives messages from the local ROUTER
        socket.
        """
        log.debug('recv_peer msg=%s', msg)
        return self.handle_recv_msg(*msg)

    def _recv_router(self, msg):
        """Greenlet that receives messages from the local ROUTER
        socket.
        """
        #log.debug('recv_router msg=%s', msg)
        return self.handle_recv_msg(*msg)

    def handle_beacon(self, peer_id, transport, addr, port, socket_type):
        """ Handle a beacon.

        Overide this method to handle new peers.  By default, connects
        a DEALER socket to the new peers broadcast endpoint and
        registers it.
        """
        #log.debug('handle_beacon peer_uuid=%s', peer_id)

        peer_addr = '%s://%s:%s' % (transport, addr, port)

        peer = self.peers.get(peer_id)
        if peer and peer.addr == peer_addr:
            peer.time = time.time()
            return
        elif peer:
            # we have the peer, but it's addr changed,
            # close it, we'll reconnect
            self.peers[peer_id].socket.close()

        #if self._debug:
        #    log.debug('peer_addr=%s', peer_addr)
        #    log.debug('peers=%s', self.peers)

        # connect DEALER to peer_addr address from beacon
        sock = self.ctx.socket(zmq.DEALER)
        sock.setsockopt(zmq.IDENTITY, self.me)

        uid = uuid.UUID(bytes=peer_id)

        log.debug('Conecting to: %s at %s' % (uid, peer_addr))
        sock.connect(peer_addr)

        sock = ZMQStream(sock, self.loop)
        sock.on_recv(self._recv_peer)
        #sock.set_close_callback(partial(self._peer_socket_closed, peer_id))

        peer = self.peers[peer_id] = self._peer_cls(peer_id, uid, sock, peer_addr, time.time(), **self._peer_init_kwargs)
        self._on_peer_connected(peer)

    #def _peer_socket_closed(self, peer_id):
    #    peer = self.peers.get(peer_id)
    #    if not peer:
    #        return
    #
    #    log.debug('Peer socket closed: %s', peer.uuid)
    #    self._on_peer_socket_closed(peer)

    def handle_recv_msg(self, peer_id, *msg):
        """Override this method to customize message handling.

        Defaults to calling the callback.
        """
        peer = self.peers.get(peer_id)
        if not peer:
            return

        #log.debug('handle_recv_msg peer=%s msg=%s', peer.uuid, msg)
        peer.time = time.time()
        self._on_recv_msg(peer, *msg)

    """
    Callbacks
    """

    def _on_recv_msg(self, peer, *msg):
        return self._callback(None, peer, *msg)

    def _on_peer_connected(self, peer):
        return self._callback(None, peer)

    def _on_peer_lost(self, peer):
        return self._callback(None, peer)

    #def _on_peer_socket_closed(self, peer):
    #    return self._callback(None, peer)

    def _callback(self, name, *args, **kwargs):
        if not name:
            name = get_last_func_name()
            if name.startswith('_on_'):
                name = name[4:]

        meth = getattr(self, 'on_%s' % name, None)
        if meth:
            meth(*args, **kwargs)
            #self.loop.add_callback(partial(meth, *args, **kwargs))

        meth = getattr(self, 'on_%s_cb' % name, None)
        if meth:
            meth(self, *args, **kwargs)
            #self.loop.add_callback(partial(meth, *args, **kwargs))
