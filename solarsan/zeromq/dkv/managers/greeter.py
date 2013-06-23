
from .base import _BaseManager
import gevent
from reflex.data import Event

import zmq.green as zmq

import uuid
import time
import socket
import errno
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


class Discovery(_BaseManager):

    debug = False
    tick_length = 5.0
    tick_timeout = 5.0
    tick_wait_until_node_ready = False

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        self.bind(self._on_peer_discovered, 'peer_discovered')

        self._node.discovery = self

    def _tick(self):
        self._send_beacon()
        self._recv_beacon()

    def discovered_peer(self, uuid):
        self.log.debug('Discovered peer')

        # TODO create Peer, add to node
        peer = 'TODO'

        self.trigger(Event('peer_discovered'), peer)

    def _on_peer_discovered(self, peer):
        self.log.debug('Event: Discovered peer %s', peer)

    """ Beacon """

    beacon_addr = ''
    beacon_port = 35713

    # TODO HACK
    service_addr = '*'
    service_port = 0
    service_transport = 'tcp'
    service_socket_type = zmq.ROUTER

    def _on_start(self):
        broadcaster = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP)
        broadcaster.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_BROADCAST,
            2)
        broadcaster.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1)
        broadcaster.setblocking(0)
        broadcaster.bind((self.beacon_addr, self.beacon_port))
        self.broadcaster = broadcaster

        # TODO HACK
        if self.service_addr != '*':
            self.service_addr_bytes = socket.inet_aton(
                socket.gethostbyname(self.service_addr))
        else:
            self.service_addr_bytes = NULL_IP

        #self._node._add_sock(self.broadcaster, self._on_broadcaster_received)

        #gevent.spawn(self._send_loop)
        #gevent.spawn(self._recv_loop)

    def _on_broadcaster_received(self, raw_parts):
        self.log.debug('Received beacon')
        return

    beacon_interval = 5.0

    def _send_loop(self):
        while True:
            self._send_beacon()
            gevent.sleep(self.beacon_interval)

    def _send_beacon(self):
        """sends udp beacons at intervals.
        """
        self.log.debug('Sending discovery beacon')

        beacon = beaconv2.pack(
            'ZRE', 2, self._node.uuid,
            self.service_port,
            T_TO_I[self.service_transport],
            self.service_socket_type,
            self.service_addr_bytes)

        try:
            self.broadcaster.sendto(
                beacon,
                ('<broadcast>', self.beacon_port))
        except socket.error as e:
            if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                self.log.exception('Error sending beacon:', e)
                raise e
            return

        # TODO Check for losts where again?
        #self._check_for_losts()

    def _recv_loop(self):
        while True:
            self._recv_beacon()
            gevent.sleep(0.1)

    def _recv_beacon(self):
        """received udp beacons
        """
        while True:
            try:
                data, (peer_addr, port) = self.broadcaster.recvfrom(
                    beaconv2.size)
            except socket.error as e:
                if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                    self.log.exception('Error recving beacon:', e)
                    raise e
                return

            try:
                if len(data) == beaconv1.size:
                    greet, ver, peer_id, peer_port = beaconv2.unpack(data)
                    # greet, ver, peer_id, peer_port = beaconv1.unpack(data)
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

            if peer_id == self._node.uuid:
                continue

            if peer_socket_address != NULL_IP:
                peer_addr = socket.inet_ntoa(peer_socket_address)

            peer_transport = I_TO_T[peer_transport]

            self.handle_beacon(peer_id, peer_transport, peer_addr,
                               peer_port, peer_socket_type)

    def handle_beacon(self, *args):
        self.log.debug('args=%s', args)

    def _check_for_losts(self):
        # check for losts
        now = time.time()
        for peer_id in self.peers.keys():
            peer = self.peers.get(peer_id)
            if not peer:
                continue

            if now - peer.time > self.dead_interval:
                self.log.debug('Lost peer %s.', peer.uuid)

                peer.socket.close()
                self._on_peer_lost(peer)

                del self.peers[peer_id]


# class _Beacon(gevent.Greenlet, LogMixin):

#    send_every = 5.0

#    def __init__(self):
#        self.running = False

#    def start(self):
#        self.log.debug('Starting beacon %s', self)
#        return gevent.Greenlet.start(self)

#    def _run(self):
#        self.running = True
#        while self.running:
#            self.send()
#            gevent.sleep(self.send_every)

#    def send(self):
#        self.log.debug('Sending beacon')
# TODO send beacon


# class _DiscoveryMixin:
#    def __init__(self):
#        self.beacon = _Beacon()

# def start(self):
# self.beacon.start()


class Greeter(_BaseManager):

    debug = False

    # TODO event on connect, use that to start sending greets
    # TODO Start greeter manager ticks, send greets every second until
    # we get one back.

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        self.bind(self._on_peer_connected, 'peer_connected')

        self._node.greeter = self

    def _on_peer_connected(self, event, peer):
        self.log.debug('Event: %s is connected', peer)
        gevent.spawn(self.greet, peer)

    """ Greet """

    def greet(self, peer, is_reply=False):
        self.log.debug('Greeting %s is_reply=%s', self, is_reply)
        self.unicast(peer, 'greet', is_reply, self._node.uuid)

    def receive_greet(self, peer, is_reply, node_uuid, *args, **kwargs):
        # Temp hackery for debug log
        args = [is_reply, node_uuid]
        args.extend(args)

        channel = self.channel
        self.log.debug(
            'Received greet peer=%s args=%s kwargs=%s channel=%s', peer, args, kwargs, channel)

        peer.receive_greet()

        # if not is_reply:
        if True:
            self.greet(peer, is_reply=True)


class Syncer(_BaseManager):

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        self.bind(self._on_peer_syncing, 'peer_syncing')

        self._node.greeter = self

    def _on_peer_syncing(self, event, peer):
        self.log.debug('Event: %s peer=%s', event, peer)
        self.peer_sync(peer)

    def peer_sync(self, peer):
        self.log.debug('Syncing %s', peer)

        sync = self._node.kv.store.copy()
        self.log.debug('sync=%s', sync)
        self.unicast(peer, 'sync', sync)

    def receive_sync(self, peer, sync, *args, **kwargs):
        self.log.debug(
            'Received SYNC from %s: sync=%s args=%s kwargs=%s', peer, sync, args, kwargs)

        self.trigger(Event('peer_synced'), peer)
        gevent.spawn_later(2, peer._synced)
