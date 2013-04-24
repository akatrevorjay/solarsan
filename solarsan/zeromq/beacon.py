
if __name__ == '__main__':
    from gevent import monkey
    monkey.patch_all()

from solarsan import logging, conf
logger = logging.getLogger(__name__)
log = logger

from solarsan.utils.stack import get_last_func_name

import socket
import struct
#import errno
import uuid
import time
#from collections import namedtuple

import gevent
from zmq import green as zmq

## Serializers
#import zmq.utils.jsonapi as json
#try:
#    import cPickle as pickle
#except ImportError:
#    import pickle

#from .util import BloscPickle
from .util import ZippedPickle


beaconv1 = struct.Struct('3sB16sH')
beaconv2 = struct.Struct('3sB16sHBB4s')

T_TO_I = {
    #'udp': 1,
    'tcp': 1,
    'pgm': 2,
}

I_TO_T = {v: k for k, v in T_TO_I.items()}

NULL_IP = '\x00' * 4


#Peer = namedtuple('Peer', ['socket', 'addr', 'time'])


class Beacon(object):
    """ZRE beacon emmiter.  http://rfc.zeromq.org/spec:20

    This implements only the base UDP beaconing 0mq socket
    interconnection layer, and disconnected peer detection.
    """
    ctx = None

    _debug = False

    service_port = None

    def __init__(self,
                 broadcast_addr='',
                 broadcast_port=35713,
                 service_addr='*',
                 service_transport='tcp',
                 service_socket_type=zmq.ROUTER,
                 beacon_interval=1,
                 dead_interval=30,
                 on_recv_msg=None,
                 on_peer_connected=None,
                 on_peer_deadbeat=None):

        self.broadcast_addr = broadcast_addr
        self.broadcast_port = broadcast_port
        self.service_addr = service_addr
        self.service_transport = service_transport
        self.service_socket_type = service_socket_type
        self.beacon_interval = beacon_interval
        self.dead_interval = dead_interval

        self.on_recv_msg_cb = on_recv_msg
        self.on_peer_connected_cb = on_peer_connected
        self.on_peer_deadbeat_cb = on_peer_deadbeat

        self.peers = {}
        if service_addr != '*':
            self.service_addr_bytes = socket.inet_aton(
                socket.gethostbyname(service_addr))
        else:
            self.service_addr_bytes = NULL_IP

        self.me = uuid.uuid4().bytes

    def start(self):
        """Greenlet to start the beaconer.  This sets up zmq context,
        sockets, and spawns worker greenlets.
        """
        if not self.ctx:
            self.ctx = zmq.Context.instance()

        self.router = self.ctx.socket(self.service_socket_type)
        endpoint = '%s://%s' % (self.service_transport, self.service_addr)
        self.service_port = self.router.bind_to_random_port(endpoint)

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

        self.broadcaster.bind((self.broadcast_addr, self.broadcast_port))

        # start all worker greenlets
        gevent.joinall(
            [gevent.spawn(self._send_beacon),
             gevent.spawn(self._recv_beacon),
             gevent.spawn(self._recv_msg)])

    def _recv_beacon(self):
        """Greenlet that receives udp beacons.
        """
        while True:
            #gevent.sleep(0)

            try:
                data, (peer_addr, port) = self.broadcaster.recvfrom(beaconv2.size)
            except socket.error:
                log.exception('Error recving beacon:')
                gevent.sleep(self.beacon_interval)  # don't busy error loop
                continue
            try:
                if len(data) == beaconv1.size:
                    greet, ver, peer_id, peer_port = beaconv2.unpack(data)
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
        while True:
            #gevent.sleep(0)

            try:
                beacon = beaconv2.pack(
                    'ZRE', 2, self.me,
                    self.service_port,
                    T_TO_I[self.service_transport],
                    self.service_socket_type,
                    self.service_addr_bytes)

                self.broadcaster.sendto(
                    beacon,
                    ('<broadcast>', self.broadcast_port))
            except socket.error:
                log.exception('Error sending beacon:')

            gevent.sleep(self.beacon_interval)

            # check for deadbeats
            now = time.time()
            for peer_id in self.peers.keys():
                peer = self.peers.get(peer_id)
                if not peer:
                    continue

                if now - peer.time > self.dead_interval:
                    log.debug('Deadbeat: %s' % uuid.UUID(bytes=peer_id))
                    peer.socket.close()

                    self._on_peer_deadbeat(peer)

                    del self.peers[peer_id]

    def _recv_msg(self):
        """Greenlet that receives messages from the local ROUTER
        socket.
        """
        while True:
            #gevent.sleep(0)

            self.handle_recv_msg(*self.router.recv_multipart())
            #peer_id = self.router.recv()
            #self.handle_recv_msg(peer_id, self.router)

    def handle_beacon(self, peer_id, transport, addr, port, socket_type):
        """ Handle a beacon.

        Overide this method to handle new peers.  By default, connects
        a DEALER socket to the new peers broadcast endpoint and
        registers it.
        """
        peer_addr = '%s://%s:%s' % (transport, addr, port)

        if self._debug:
            log.debug('peer_addr=%s', peer_addr)

        peer = self.peers.get(peer_id)
        if peer and peer.addr == peer_addr:
            peer.time = time.time()
            return
        elif peer:
            # we have the peer, but it's addr changed,
            # close it, we'll reconnect
            self.peers[peer_id].socket.close()

        if self._debug:
            log.debug('peers=%s', self.peers)

        # connect DEALER to peer_addr address from beacon
        peer = self.ctx.socket(zmq.DEALER)
        peer.setsockopt(zmq.IDENTITY, self.me)

        uid = uuid.UUID(bytes=peer_id)
        log.info('Conecting to: %s at %s' % (uid, peer_addr))
        peer.connect(peer_addr)
        self.peers[peer_id] = Peer(peer_id, uid, peer, peer_addr, time.time())

        peer = self.peers.get(peer_id)
        if peer:
            self._on_peer_connected(peer)

    def handle_recv_msg(self, peer_id, *msg):
        """Override this method to customize message handling.

        Defaults to calling the callback.
        """
        peer = self.peers.get(peer_id)
        if peer:
            peer.time = time.time()
            self._on_recv_msg(peer, *msg)

    """
    Callbacks
    """

    def _on_recv_msg(self, peer, *msg):
        return self._callback(None, peer, *msg)

    def _on_peer_connected(self, peer):
        return self._callback(None, peer)

    def _on_peer_deadbeat(self, peer):
        return self._callback(None, peer)

    def _callback(self, name, *args, **kwargs):
        if not name:
            name = get_last_func_name()
            if name.startswith('_on_'):
                name = name[4:]

        meth = getattr(self, 'on_%s' % name, None)
        if meth:
            #gevent.spawn(meth, *args, **kwargs)
            meth(*args, **kwargs)

        meth = getattr(self, 'on_%s_cb' % name, None)
        if meth:
            gevent.spawn(meth, self, *args, **kwargs)


import solarsan.cluster.models as cmodels


class Greet:
    hostname = None
    uuid = None
    cluster_iface = None

    def __init__(self):
        pass

    @classmethod
    def gen_from_peer(cls, peer):
        self = Greet()
        self.hostname = peer.hostname
        self.uuid = peer.uuid
        self.cluster_iface = peer.cluster_iface
        return self

    @classmethod
    def gen_from_local(cls):
        peer = cmodels.Peer.get_local()
        return cls.gen_from_peer(peer)


class Peer:
    id = None
    uuid = None
    socket = None
    addr = None
    time = None

    def __init__(self, id, uuid, socket, addr, time):
        self.id = id
        self.uuid = uuid
        self.socket = socket
        self.addr = addr
        self.time = time

        #self.state = self.states.initial

    #class states:
    #    initial = 'initial'
    #    greet = 'greet'

    greet = None

    def send_greet(self):
        logger.debug('Peer %s: Sending Greet', self.uuid)
        greet = Greet.gen_from_local()
        greet = ZippedPickle.dump(greet)
        self.socket.send_multipart(['GREET', greet])

        #if self.state == self.states.initial:
        #    self.state = self.states.greet

    def on_greet(self, z):
        self.greet = ZippedPickle.load(z)
        logger.debug('Peer %s: Got Greet %s', self.uuid, self.greet.__dict__)


class GreeterBeacon(Beacon):
    def on_recv_msg(self, peer, *msg):
        cmd = msg[0]
        #log.debug('Peer %s: %s (state=%s)', peer.uuid, cmd, peer.state)
        #log.debug('msg=%s', msg)

        if cmd == 'GREET':
            peer.on_greet(msg[1])
        else:
            logger.error('Peer %s: Wtfux %s?', peer.uuid, cmd)
            peer.socket.close()

    def on_peer_connected(self, peer):
        log.info('Peer %s: Connected.', peer.uuid)

        # The connectee takes a small amount of time before it actaully accepts
        # anything, otherwise it kinda just gets "lost".
        gevent.sleep(self.beacon_interval)

        peer.send_greet()

    def on_peer_deadbeat(self, peer):
        log.info('Peer %s: Lost.', peer.uuid)


if __name__ == '__main__':
    local = cmodels.Peer.get_local()
    ipaddr = str(local.cluster_nic.ipaddr)

    log.info('Starting Beacon (service_addr=%s, discovery_port=%s)',
             ipaddr, conf.ports.discovery)

    p = GreeterBeacon(
        beacon_interval=2,
        #beacon_interval=1,
        dead_interval=2,
        service_addr=ipaddr,
        #broadcast_addr=bcast,
        broadcast_port=conf.ports.discovery,
    )

    g = gevent.spawn(p.start)

    log.info('Started.')

    g.join()
