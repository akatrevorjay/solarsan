
from solarsan import logging, conf
log = logging.getLogger(__name__)
from solarsan.utils.stack import get_last_func_name
from solarsan.pretty import pp
import solarsan.cluster.models as cmodels

import time
import threading
from functools import partial

import zmq
#from zmq import ZMQError
from zmq.eventloop.ioloop import IOLoop, DelayedCallback, PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream
#import zmq.utils.jsonapi as json

from .beacon import Beacon
from .bstar import BinaryStar
from .kvmsg import KVMsg
from .zhelpers import dump

#from . import serializers
from .serializers import Pipeline, \
    PickleSerializer, JsonSerializer, MsgPackSerializer, \
    ZippedCompressor, BloscCompressor

pipeline = Pipeline()
pipeline.add(PickleSerializer())
pipeline.add(ZippedCompressor())


class Greet:
    @classmethod
    def _gen_from_peer(cls, peer):
        self = Greet()
        self.hostname = peer.hostname
        self.uuid = peer.uuid
        self.cluster_iface = peer.cluster_iface
        return self

    @classmethod
    def gen_from_local(cls):
        peer = cmodels.Peer.get_local()
        return cls._gen_from_peer(peer)


class Peer:
    ctx = None

    def __init__(self, id, uuid, socket, addr, time_=None, beacon=None, **kwargs):
        self.id = id
        self.uuid = uuid
        self.socket = socket
        self.addr = addr
        self.time = time_ or time.time()

        self.transport, host = addr.split('://', 1)
        self.host, self.beacon_router_port = host.rsplit(':', 1)

        # Set callbacks
        for k, v in kwargs.iteritems():
            if k.startswith('on_') and k.endswith('_cb'):
                setattr(self, k, v)

        if not self.ctx:
            self.ctx = zmq.Context.instance()
        self.loop = IOLoop.instance()

        # Set up our own dkv client interface to peer
        self.subscriber = self.ctx.socket(zmq.SUB)
        self.subscriber.setsockopt(zmq.SUBSCRIBE, b'')
        self.subscriber.connect(self.publisher_endpoint)

        # Wrap sockets in ZMQStreams for IOLoop handlers
        self.subscriber = ZMQStream(self.subscriber)
        self.subscriber.on_recv(self._on_subscriber_recv)

    beacon_router_port = None
    port = conf.ports.dkv
    publisher_port = conf.ports.dkv_publisher
    collector_port = conf.ports.dkv_collector

    @property
    def publisher_endpoint(self):
        return 'tcp://%s:%d' % (self.host, self.publisher_port)

    def _on_subscriber_recv(self, msg):
        #if msg[0] != 'HUGZ':
        #    log.debug('Peer %s: Subscriber msg=%s', self.uuid, msg)

        return self._callback(None, msg)

    def _callback(self, name, *args, **kwargs):
        if not name:
            name = get_last_func_name()
            if name.startswith('_on_'):
                name = name[4:]

        meth = getattr(self, 'on_%s' % name, None)
        if meth:
            #meth(*args, **kwargs)
            self.loop.add_callback(partial(meth, *args, **kwargs))

        meth = getattr(self, 'on_%s_cb' % name, None)
        if meth:
            #meth(self, *args, **kwargs)
            self.loop.add_callback(partial(meth, self, *args, **kwargs))
    """
    Dkv
    """

    @property
    def snapshot_port(self):
        return conf.ports.dkv

    @property
    def snapshot_endpoint(self):
        return 'tcp://%s:%d' % (self.host, self.snapshot_port)

    def get_snapshot(self):
        snapshot = self.ctx.socket(zmq.DEALER)
        snapshot.linger = 0

        snapshot.connect(self.snapshot_endpoint)

        log.info("Asking for snapshot from: %s", self.snapshot_endpoint)
        snapshot.send_multipart(["ICANHAZ?", ''])
        return snapshot

    """
    Greet
    """

    def send_greet(self):
        log.debug('Peer %s: Sending Greet', self.uuid)
        greet = Greet.gen_from_local()
        greet = pipeline.dump(greet)
        #greet = json.dumps(greet.__dict__)
        self.socket.send_multipart(['GREET', greet])

        delay = getattr(self, 'send_greet_delay', None)
        if delay:
            delay.stop()
            delattr(self, 'send_greet_delay')

    def _on_greet(self, serialized_obj):
        greet = pipeline.load(serialized_obj)
        #greet = Greet()
        #greet.__dict__ = json.loads(serialized_obj)

        log.debug('Peer %s: Got Greet', self.uuid)
        pp(greet.__dict__)

        self.greet = greet

        return self._callback(None, serialized_obj)


class GreeterBeacon(Beacon):
    _peer_cls = Peer
    dkvsrv = None

    def __init__(self, *args, **kwargs):
        super(GreeterBeacon, self).__init__(*args, **kwargs)

        self.dkvsrv = DkvServer()

        # Set subscriber callback
        self._peer_init_kwargs['on_subscriber_recv_cb'] = self.dkvsrv.handle_subscriber

    def start(self, loop=True):
        super(GreeterBeacon, self).start(loop=False)
        #self.dkvsrv.start(loop=False)
        if loop:
            return self.dkvsrv.start(loop=loop)
            #return self.loop.start()

    def on_recv_msg(self, peer, *msg):
        cmd = msg[0]
        log.info('Peer %s: %s.', peer.uuid, cmd)

        if cmd == 'GREET':
            peer._on_greet(msg[1])
            self.loop.add_callback(partial(self._callback, 'peer_on_greet', peer))
        else:
            log.error('Peer %s: Wtfux %s?', peer.uuid, cmd)
            peer.socket.close()

    def on_peer_connected(self, peer):
        log.info('Peer %s: Connected.', peer.uuid)

        peer.send_greet_delay = DelayedCallback(peer.send_greet, self.beacon_interval * 1000)
        peer.send_greet_delay.start()

        self.dkvsrv.add_peer(peer)

    def on_peer_lost(self, peer):
        log.info('Peer %s: Lost.', peer.uuid)

        self.dkvsrv.remove_peer(peer)


class Route:
    """Simple struct for routing information for a key-value snapshot"""
    socket = None
    identity = None
    subtree = None

    def __init__(self, socket, identity, subtree):
        self.socket = socket        # ROUTER socket to send to
        self.identity = identity    # Identity of peer who requested state
        self.subtree = subtree      # Client subtree specification


def send_single(key, kvmsg, route):
    """Send one state snapshot key-value pair to a socket"""
    # check front of key against subscription subtree:
    if kvmsg.key.startswith(route.subtree):
        # Send identity of recipient first
        route.socket.send(route.identity, zmq.SNDMORE)
        kvmsg.send(route.socket)


class DkvServer(object):
    """ Dkv Server object """
    ctx = None                  # Context wrapper

    kvmap = None                # Key-value store
    sequence = 0                # How many updates so far

    #bstar = None
    #router = None               # Router socket used for DKV

    publisher = None            # Publish updates and hugz
    collector = None            # Collect updates from clients
    pending = None              # Pending updates from client

    primary = False             # True if we're primary
    master = False              # True if we're master
    slave = False               # True if we're slave

    _debug = False

    def add_peer(self, peer):
        log.info('DkvServer: Adding Peer %s.', peer.uuid)

        uuid = peer.uuid
        self.peers[uuid] = peer

    def remove_peer(self, peer):
        log.info('DkvServer: Removing Peer %s.', peer.uuid)

        uuid = peer.uuid
        del self.peers[uuid]

    @property
    def router_endpoint(self):
        return 'tcp://%s:%d' % (self.service_addr, self.port)

    def __init__(self, service_addr='*', port=conf.ports.dkv):
        self.service_addr = service_addr
        self.port = port

        if not self.ctx:
            self.ctx = zmq.Context.instance()
        self.loop = IOLoop.instance()

        # Base init
        self.peers = {}
        self.pending = []

        """TODO Automatic voting mechanism to pick primary"""

        if conf.hostname == 'san0':
            remote_host = 'san1'
            primary = True
        elif conf.hostname == 'san1':
            remote_host = 'san0'
            primary = False
        self.remote_host = remote_host
        self.primary = primary

        if primary:
            self.kvmap = {}
            bstar_local_ep = 'tcp://*:%d' % conf.ports.bstar_primary
            bstar_remote_ep = 'tcp://%s:%d' % (remote_host, conf.ports.bstar_secondary)
        else:
            bstar_local_ep = 'tcp://*:%d' % conf.ports.bstar_secondary
            bstar_remote_ep = 'tcp://%s:%d' % (remote_host, conf.ports.bstar_primary)

        # Setup router socket
        #self.router = self.ctx.socket(zmq.ROUTER)
        #self.router.bind(self.router_endpoint)
        #self.router = ZMQStream(self.router)
        #self.router.on_recv_stream(self.handle_snapshot)

        self.bstar = BinaryStar(primary, bstar_local_ep, bstar_remote_ep)
        self.bstar.register_voter(self.router_endpoint,
                                  zmq.ROUTER,
                                  self.handle_snapshot)

        # Register state change handlers
        self.bstar.master_callback = self.become_master
        self.bstar.slave_callback = self.become_slave

        # Set up our dkv server sockets
        self.publisher = self.ctx.socket(zmq.PUB)
        address = self.publisher_endpoint
        log.debug('Binding publisher on %s', address)
        self.publisher.bind(address)
        #self.publisher = ZMQStream(self.publisher)

        self.collector = self.ctx.socket(zmq.SUB)
        self.collector.setsockopt(zmq.SUBSCRIBE, b'')
        #self.collector.setsockopt(zmq.SUBSCRIBE, '/clients/updates')
        address = self.collector_endpoint
        log.debug('Binding collector on %s', address)
        self.collector.bind(address)
        self.collector = ZMQStream(self.collector)
        self.collector.on_recv(self.handle_collect)

        # Register our handlers with reactor
        self.flush_callback = PeriodicCallback(self.flush_ttl, 1000)
        self.hugz_callback = PeriodicCallback(self.send_hugz, 1000)

    @property
    def collector_port(self):
        return conf.ports.dkv_collector

    @property
    def publisher_port(self):
        return conf.ports.dkv_publisher

    @property
    def collector_endpoint(self):
        return 'tcp://%s:%d' % (self.service_addr, conf.ports.dkv_collector)

    @property
    def publisher_endpoint(self):
        return 'tcp://%s:%d' % (self.service_addr, conf.ports.dkv_publisher)

    def start(self, loop=True):
        log.debug('DkvServer starting')

        # start periodic callbacks
        self.flush_callback.start()
        self.hugz_callback.start()

        # Start bstar reactor until process interrupted
        self.bstar.start(loop=loop)

        #if loop:
        #    self.loop.start()

    def handle_snapshot(self, socket, msg):
        """snapshot requests"""
        #log.debug('handle_snapshot: Got msg=%s', msg[1:])

        if msg[1] != "ICANHAZ?" or len(msg) != 3:
            log.error("bad request, aborting")
            dump(msg)
            #self.bstar.loop.stop()
            return

        identity, request = msg[:2]

        if len(msg) >= 3:
            subtree = msg[2]
            # Send state snapshot to client
            route = Route(socket, identity, subtree)

            # For each entry in kvmap, send kvmsg to client
            for k, v in self.kvmap.items():
                send_single(k, v, route)

            # Now send END message with sequence number
            log.info("Sending state shapshot=%d", self.sequence)
            socket.send(identity, zmq.SNDMORE)
            kvmsg = KVMsg(self.sequence)
            kvmsg.key = "KTHXBAI"
            kvmsg.body = subtree
            kvmsg.send(socket)

    def handle_collect(self, msg):
        """Collect updates from clients

        If we're master, we apply these to the kvmap
        If we're slave, or unsure, we queue them on our pending list
        """
        if self._debug:
            log.debug('msg=%s', msg)

        #if len(msg) != 5:
        #    log.info('handle_collect: Got bad message %s.', msg)
        #    return

        kvmsg = KVMsg.from_msg(msg)
        if self.master:
            self.sequence += 1
            kvmsg.sequence = self.sequence
            kvmsg.send(self.publisher)
            ttl = kvmsg.get('ttl')
            if ttl:
                kvmsg['ttl'] = time.time() + ttl
            kvmsg.store(self.kvmap)
            log.info("publishing update=%d", self.sequence)
        else:
            # If we already got message from master, drop it, else
            # hold on pending list
            if not self.was_pending(kvmsg):
                self.pending.append(kvmsg)

    def was_pending(self, kvmsg):
        """If message was already on pending list, remove and return True.
        Else return False.
        """
        found = False
        for idx, held in enumerate(self.pending):
            if held.uuid == kvmsg.uuid:
                found = True
                break
        if found:
            self.pending.pop(idx)
        return found

    def flush_ttl(self):
        """Purge ephemeral values that have expired"""
        if self.kvmap:
            for key, kvmsg in self.kvmap.items():
                self.flush_single(kvmsg)

    def flush_single(self, kvmsg):
        """If key-value pair has expired, delete it and publish the fact
        to listening clients."""
        ttl = kvmsg.get('ttl')
        if ttl and ttl <= time.time():
            kvmsg.body = ""
            self.sequence += 1
            kvmsg.sequence = self.sequence
            kvmsg.send(self.publisher)
            del self.kvmap[kvmsg.key]
            log.debug("publishing delete=%d", self.sequence)

    def send_hugz(self):
        """Send hugz to anyone listening on the publisher socket"""
        if self._debug:
            log.debug('Sending HUGZ to publisher')

        kvmsg = KVMsg(self.sequence)
        kvmsg.key = "HUGZ"
        kvmsg.body = ""
        kvmsg.send(self.publisher)

    """
    State change handlers
    """

    def become_master(self):
        """We're becoming master

        The backup server applies its pending list to its own hash table,
        and then starts to process state snapshot requests.
        """
        log.info('Becoming master')

        self.master = True
        self.slave = False

        # stop receiving subscriber updates while we are master
        for peer in self.peers.values():
            peer.subscriber.stop_on_recv()

        # Apply pending list to own kvmap
        while self.pending:
            kvmsg = self.pending.pop(0)
            self.sequence += 1
            kvmsg.sequence = self.sequence
            kvmsg.store(self.kvmap)
            log.info("publishing pending=%d", self.sequence)

    def become_slave(self):
        """We're becoming slave"""
        log.info('Becoming slave')

        self.kvmap = None     # clear kvmap

        self.master = False
        self.slave = True

        # start receiving subscriber updates
        for peer in self.peers.values():
            peer.subscriber.on_recv(peer._on_subscriber_recv)

    def handle_subscriber(self, peer, msg):
        """Collect updates from peer (master)
        We're always slave when we get these updates
        """
        if self.master:
            if msg[0] != 'HUGZ':
                log.warn(
                    "Received subscriber message, but we are master msg=%s from_peer=%s", msg, peer.uuid)
                return

        # Get state snapshot if necessary
        if self.kvmap is None:
            self.kvmap = {}

            snapshot = peer.get_snapshot()
            while True:
                try:
                    kvmsg = KVMsg.recv(snapshot)
                except KeyboardInterrupt:
                    #self.bstar.loop.stop()
                    self.loop.stop()
                    return

                if kvmsg.key == "KTHXBAI":
                    self.sequence = kvmsg.sequence
                    break          # Done

                kvmsg.store(self.kvmap)

            log.info("received snapshot=%d", self.sequence)

        ##if not isinstance(msg, (tuple, list)) or len(msg) < 5:
        #if not len(msg) == 5:
        #    log.debug('bad msg=%s', msg)
        #    return

        # Find and remove update off pending list
        kvmsg = KVMsg.from_msg(msg)

        # update integer ttl -> timestamp
        ttl = kvmsg.get('ttl')
        if ttl is not None:
            kvmsg['ttl'] = time.time() + ttl

        if kvmsg.key != "HUGZ":
            if not self.was_pending(kvmsg):
                """ If master update came before client update, flip it
                around, store master update (with sequence) on pending
                list and use to clear client update when it comes later.
                """
                self.pending.append(kvmsg)

            # If update is more recent than our kvmap, apply it
            if (kvmsg.sequence > self.sequence):
                self.sequence = kvmsg.sequence
                kvmsg.store(self.kvmap)
                log.info("received update=%d", self.sequence)


def main():
    local = cmodels.Peer.get_local()
    ipaddr = str(local.cluster_nic.ipaddr)

    log.info('Starting Beacon (service_addr=%s, discovery_port=%s)',
             ipaddr, conf.ports.discovery)

    gb = GreeterBeacon(
        #beacon_interval=2,
        beacon_interval=1,
        dead_interval=10,
        service_addr=ipaddr,
        #broadcast_addr=bcast,
        broadcast_port=conf.ports.discovery,
    )

    try:
        #gb.start()

        t = threading.Thread(target=gb.start)
        t.daemon = True
        t.start()
        log.info('Started.')

        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == '__main__':
    main()
