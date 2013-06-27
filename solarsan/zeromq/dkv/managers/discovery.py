
from .base import _BaseManager
import gevent
from reflex.data import Event

import zmq.green as zmq

import time
import socket
import errno
import struct


beaconv3 = struct.Struct('8sB40sHBB4s')

T_TO_I = {
    'udp': 3,
    'tcp': 1,
    'pgm': 2,
}

I_TO_T = {v: k for k, v in T_TO_I.items()}

NULL_IP = '\x00' * 4

from ..utils import ZmqEndpoint


class Discovery(_BaseManager):

    debug = False
    tick_length = 1.0
    tick_timeout = 5.0
    tick_wait_until_node_ready = False

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        self._node.discovery = self

    def _tick(self):
        self._send_beacon()

    def discovered_peer(self, uuid):
        self.log.debug('Discovered peer')

        # TODO create Peer, add to node
        peer = 'TODO'

        self.trigger(Event('peer_discovered'), peer)

    """ Beacon """

    beacon_addr = ''
    beacon_port = 35713

    # TODO HACK
    service_addr = '*'
    service_port = None
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
        # Non blocking; works but we don't want it in this case.
        # broadcaster.setblocking(0)
        broadcaster.bind((self.beacon_addr, self.beacon_port))
        self.broadcaster = broadcaster

        # TODO HACK
        if self.service_addr != '*':
            self.service_addr_bytes = socket.inet_aton(
                socket.gethostbyname(self.service_addr))
        else:
            self.service_addr_bytes = NULL_IP

        self.service_port = self._node.rtr_endpoint.port

        gevent.spawn(self._recv_beacon)

    def _on_broadcaster_received(self, raw_parts):
        self.log.debug('Received beacon')
        return

    def _send_beacon(self):
        """sends udp beacons at intervals.
        """
        # self.log.debug('Sending discovery beacon')

        beacon = beaconv3.pack(
            'SolarSan', 3, self._node.uuid,
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
        # self._check_for_losts()

    def _recv_beacon(self):
        """received udp beacons
        """
        while True:
            try:
                data, (peer_addr, port) = self.broadcaster.recvfrom(
                    beaconv3.size)
            except socket.error as e:
                if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                    self.log.exception('Error recving beacon:', e)
                    raise e
                return

            try:
                greet, ver, peer_id, peer_port, \
                    peer_transport, peer_socket_type, \
                    peer_socket_address = beaconv3.unpack(data)
                if ver != 3:
                    continue
            except Exception:
                continue

            if greet != 'SolarSan':
                continue

            if str(peer_id) == self._node.uuid:
                continue

            if peer_socket_address != NULL_IP:
                peer_addr = socket.inet_ntoa(peer_socket_address)

            peer_transport = I_TO_T[peer_transport]

            gevent.spawn(
                self.handle_beacon, str(peer_id), str(
                    peer_transport), str(peer_addr),
                int(peer_port), int(peer_socket_type))

            # gevent.sleep(0)

    def handle_beacon(self, peer_uuid, socket_transport, socket_host, socket_port, socket_type):
        if peer_uuid in self._node.peers:
            # We already have this peer
            return
        peer_endpoint = ZmqEndpoint(
            transport=socket_transport, host=socket_host, port=socket_port, socket_type=socket_type)
        gevent.spawn(self.trigger, Event(
            'peer_discovered'), peer_uuid, peer_endpoint)

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
