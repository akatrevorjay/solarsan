
if __name__ == '__main__':
    from gevent import monkey
    monkey.patch_all()

from solarsan import logging, conf
logger = logging.getLogger(__name__)

from solarsan.utils.stack import get_last_func_name

import socket
import struct
#import errno
import uuid
import time
from collections import namedtuple

import gevent
from zmq import green as zmq

beaconv1 = struct.Struct('3sB16sH')
beaconv2 = struct.Struct('3sB16sHBB4s')

Peer = namedtuple('Peer', ['socket', 'addr', 'time'])

log = logger

T_TO_I = {
    #'udp': 1,
    'tcp': 1,
    'pgm': 2,
}

I_TO_T = {v: k for k, v in T_TO_I.items()}

NULL_IP = '\x00' * 4


class Beaconer(object):
    """ZRE beacon emmiter.  http://rfc.zeromq.org/spec:20

    This implements only the base UDP beaconing 0mq socket
    interconnection layer, and disconnected peer detection.
    """
    service_port = None
    _debug = False

    def __init__(self,
                 broadcast_addr='',
                 broadcast_port=35713,
                 service_addr='*',
                 service_transport='tcp',
                 service_socket_type=zmq.ROUTER,
                 beacon_interval=1,
                 dead_interval=30):
                 on_msg=None,
                 on_peer_connected=None,
                 on_peer_deadbeat=None):

        self.broadcast_addr = broadcast_addr
        self.broadcast_port = broadcast_port
        self.service_addr = service_addr
        self.service_transport = service_transport
        self.service_socket_type = service_socket_type
        self.beacon_interval = beacon_interval
        self.dead_interval = dead_interval

        if on_msg:
            self.on_msg_cb = on_msg
        if on_peer_connected:
            self.on_peer_connected_cb = on_peer_connected
        if on_peer_deadbeat:
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
        self.context = zmq.Context()
        self.router = self.context.socket(self.service_socket_type)
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
                peer = self.peers[peer_id]
                if now - peer.time > self.dead_interval:
                    log.debug('Deadbeat: %s' % uuid.UUID(bytes=peer_id))
                    peer.socket.close()

                    self._on_peer_deadbeat(self.peers[peer_id])

                    del self.peers[peer_id]

    def _recv_msg(self):
        """Greenlet that receives messages from the local ROUTER
        socket.
        """
        while True:
            self.handle_msg(*self.router.recv_multipart())

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
            self.peers[peer_id] = peer._replace(time=time.time())
            return
        elif peer:
            # we have the peer, but it's addr changed,
            # close it, we'll reconnect
            self.peers[peer_id].socket.close()

        if self._debug:
            log.debug('peers=%s', self.peers)

        # connect DEALER to peer_addr address from beacon
        peer = self.context.socket(zmq.DEALER)
        peer.setsockopt(zmq.IDENTITY, self.me)

        uid = uuid.UUID(bytes=peer_id)
        log.debug('conecting to: %s at %s' % (uid, peer_addr))
        peer.connect(peer_addr)
        self.peers[peer_id] = Peer(peer, peer_addr, time.time())

        self._on_peer_connected(self.peers[peer_id])

    def handle_msg(self, peer_id, msg):
        """Override this method to customize message handling.

        Defaults to calling the callback.
        """
        peer = self.peers.get(peer_id)
        if peer:
            self.peers[peer_id] = peer = peer._replace(time=time.time())
            self._on_msg(peer, msg)

    """
    Callbacks
    """

    on_msg = None
    on_peer_connected = None
    on_peer_deadneat = None

    def _on_msg(self, peer, msg):
        return self._callback(None, peer, msg)

    def _on_peer_connected(self, peer):
        return self._callback(None, peer)

    def _on_peer_deadbeat(self, peer):
        return self._callback(None, peer)

    def _callback(self, name, *args, **kwargs):
        if not name:
            name = get_last_func_name()
            if name.startswith('_'):
                name = name[1:]

        meth = getattr(self, name, None)
        if meth:
            #gevent.spawn(meth, *args, **kwargs)
            meth(*args, **kwargs)

        meth = getattr(self, '%s_cb' % name, None)
        if meth:
            gevent.spawn(meth, self, *args, **kwargs)


class Beaconer2(Beaconer):
    def on_msg(self, peer, msg):
        log.info('got msg=%s peer=%s', msg, peer)
        #log.info('self=%s', self)
        #gevent.sleep(1)

        if msg == 'SUP':
            logger.debug('sending WAT')
            peer.socket.send('WAT')
        elif msg == 'WAT':
            logger.debug('got final WAT mofo')

    def on_peer_connected(self, peer):
        log.info('connected peer=%s', peer)

        # Send test message soon as we're connected
        logger.debug('sending SUP')
        peer.socket.send('SUP')

    def on_peer_deadbeat(self, peer):
        log.info('deadbeat peer=%s', peer)


if __name__ == '__main__':
    import sys

    def on_msg_cb(pyre, peer, msg):
        log.info('got msg=%s peer=%s', msg, peer)
        #log.info('pyre=%s', pyre)
        #gevent.sleep(1)

        if msg == 'SUP':
            logger.debug('sending WAT')
            peer.socket.send('WAT')
        elif msg == 'WAT':
            logger.debug('got final WAT mofo')

    def on_peer_connected_cb(pyre, peer):
        log.info('connected peer=%s', peer)

        # Send test message soon as we're connected
        logger.debug('sending SUP')
        peer.socket.send('SUP')

    def on_peer_deadbeat_cb(pyre, peer):
        log.info('deadbeat peer=%s', peer)

    import solarsan.cluster.models as cmodels
    local = cmodels.Peer.get_local()
    ipaddr = str(local.cluster_nic.ipaddr)

    log.info('service_addr=%s discovery_port=%s',
             ipaddr, conf.ports.discovery)
    log.info('Starting')

    p = Beaconer2(
        #beacon_interval=10,
        beacon_interval=1,
        dead_interval=10,
        service_addr=ipaddr,
        #broadcast_addr=bcast,
        broadcast_port=conf.ports.discovery,
        #on_msg=on_msg_cb,
        #on_peer_connected=on_peer_connected_cb,
        #on_peer_deadbeat=on_peer_deadbeat_cb,
    )

    g = gevent.spawn(p.start)
    s = gevent.sleep

    if len(sys.argv) > 1:
        g.join()
