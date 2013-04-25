
from solarsan import logging, conf
log = logging.getLogger(__name__)
#from solarsan.utils.stack import get_last_func_name
from solarsan.pretty import pp
import solarsan.cluster.models as cmodels

import time
import threading
from functools import partial

import zmq
#from zmq import ZMQError
from zmq.eventloop.ioloop import IOLoop, DelayedCallback, PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream
import zmq.utils.jsonapi as json

#from .util import ZippedPickle
from .beacon import Beacon


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

    def __init__(self, id, uuid, socket, addr, time_=None, beacon=None, clonesrv=None):
        self.id = id
        self.uuid = uuid
        self.socket = socket
        self.addr = addr
        self.time = time_ or time.time()

        self.proto, host = addr.split('://', 1)
        self.host, port = host.rsplit(':', 1)
        #self.host = self.addr.rsplit(':', 1)[0].split('://', 1)[1]

        # TODO This does not belong here.
        self.on_subscriber_recv_cb = clonesrv.handle_subscriber

        self.init()

    def init(self):
        if not self.ctx:
            self.ctx = zmq.Context.instance()
        self.loop = IOLoop.instance()

        # Set up our own clone client interface to peer
        self.subscriber = self.ctx.socket(zmq.SUB)
        self.subscriber.setsockopt(zmq.SUBSCRIBE, b'')
        self.subscriber.connect(self.subscriber_endpoint)

        # Wrap sockets in ZMQStreams for IOLoop handlers
        self.subscriber = ZMQStream(self.subscriber, self.loop)
        self.subscriber.on_recv(self.subscriber_recv)

    port = 5556

    @property
    def collector_port(self):
        return self.port + 1

    @property
    def subscriber_port(self):
        return self.port + 2

    @property
    def subscriber_endpoint(self):
        return 'tcp://%s:%d' % (self.host, self.subscriber_port)

    def subscriber_recv(self, msg):
        if msg[0] != 'HUGZ':
            log.debug('Peer %s: Subscriber msg=%s', self.uuid, msg)
        if self.on_subscriber_recv_cb:
            self.on_subscriber_recv_cb(self, msg)

    """
    Clone
    """

    @property
    def snapshot_endpoint(self):
        return 'tcp://%s:%d' % (self.host, self.collector_port)

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
        #greet = ZippedPickle.dump(greet)
        greet = json.dumps(greet.__dict__)
        self.socket.send_multipart(['GREET', greet])

        delay = getattr(self, 'send_greet_delay', None)
        if delay:
            delay.stop()
            delattr(self, 'send_greet_delay')

    def on_greet(self, serialized_obj):
        #greet = ZippedPickle.load(z)
        greet = Greet()
        greet.__dict__ = json.loads(serialized_obj)

        log.debug('Peer %s: Got Greet', self.uuid)
        pp(greet.__dict__)

        self.greet = greet


class GreeterBeacon(Beacon):
    _peer_cls = Peer
    clonesrv = None

    def __init__(self, *args, **kwargs):
        super(GreeterBeacon, self).__init__(*args, **kwargs)

        self.clonesrv = CloneServer()
        self._peer_init_kwargs['clonesrv'] = self.clonesrv

    def start(self, loop=True):
        super(GreeterBeacon, self).start(loop=False)
        #self.clonesrv.start(loop=False)
        if loop:
            return self.clonesrv.start(loop=loop)
            #return self.loop.start()

    def on_recv_msg(self, peer, *msg):
        cmd = msg[0]
        #log.debug('Peer %s: %s (state=%s)', peer.uuid, cmd, peer.state)
        #log.debug('msg=%s', msg)

        if cmd == 'GREET':
            peer.on_greet(msg[1])
            self.loop.add_callback(partial(self._callback, 'peer_on_greet', peer))
        else:
            log.error('Peer %s: Wtfux %s?', peer.uuid, cmd)
            peer.socket.close()

    def on_peer_connected(self, peer):
        log.info('Peer %s: Connected.', peer.uuid)

        peer.send_greet_delay = DelayedCallback(peer.send_greet, self.beacon_interval * 1000, self.loop)
        peer.send_greet_delay.start()

        self.clonesrv.add_peer(peer)

    def on_peer_lost(self, peer):
        log.info('Peer %s: Lost.', peer.uuid)

        self.clonesrv.remove_peer(peer)


class FSMError(Exception):
    """Exception class for invalid state"""
    pass


class BinaryStar(object):
    class STATES:
        """States we can be in at any point in time"""
        PRIMARY = 1          # Primary, waiting for peer to connect
        BACKUP = 2           # Backup, waiting for peer to connect
        ACTIVE = 3           # Active - accepting connections
        PASSIVE = 4          # Passive - not accepting connections

    class EVENTS:
        """Events, which start with the states our peer can be in"""
        PEER_PRIMARY = 1           # HA peer is pending primary
        PEER_BACKUP = 2            # HA peer is pending backup
        PEER_ACTIVE = 3            # HA peer is active
        PEER_PASSIVE = 4           # HA peer is passive
        CLIENT_REQUEST = 5         # Client makes request

    # We send state information every this often
    # If peer doesn't respond in two heartbeats, it is 'dead'
    HEARTBEAT = 1000          # In msecs

    ctx = None              # Our private context
    loop = None             # Reactor loop
    statepub = None         # State publisher
    statesub = None         # State subscriber
    state = None            # Current state
    event = None            # Current event
    peer_expiry = 0         # When peer is considered 'dead'
    voter_callback = None   # Voting socket handler
    master_callback = None  # Call when become master
    slave_callback = None   # Call when become slave
    heartbeat = None        # PeriodicCallback for

    _debug = False

    def __init__(self, primary, local, remote):
        if not self.ctx:
            self.ctx = zmq.Context.instance()

        # initialize the Binary Star
        self.loop = IOLoop.instance()
        self.state = self.STATES.PRIMARY if primary else self.STATES.BACKUP

        # Create publisher for state going to peer
        self.statepub = self.ctx.socket(zmq.PUB)
        self.statepub.bind(local)

        # Create subscriber for state coming from peer
        self.statesub = self.ctx.socket(zmq.SUB)
        self.statesub.setsockopt(zmq.SUBSCRIBE, '')
        self.statesub.connect(remote)

        # wrap statesub in ZMQStream for event triggers
        self.statesub = ZMQStream(self.statesub, self.loop)

        # setup basic reactor events
        self.heartbeat = PeriodicCallback(
            self.send_state, self.HEARTBEAT, self.loop)
        self.statesub.on_recv(self.recv_state)

    def update_peer_expiry(self):
        """Update peer expiry time to be 2 heartbeats from now."""
        self.peer_expiry = time.time() + 2e-3 * self.HEARTBEAT

    def start(self, loop=True):
        """Start Binary Star loop"""
        self.update_peer_expiry()
        self.heartbeat.start()
        if loop:
            return self.loop.start()

    def execute_fsm(self):
        """Binary Star finite state machine (applies event to state)

        returns True if connections should be accepted, False otherwise.
        """
        accept = True
        if (self.state == self.STATES.PRIMARY):
            # Primary server is waiting for peer to connect
            # Accepts self.EVENTS.CLIENT_REQUEST events in this state
            if (self.event == self.EVENTS.PEER_BACKUP):
                log.info("connected to backup (slave), ready as master")
                self.state = self.STATES.ACTIVE
                if (self.master_callback):
                    self.loop.add_callback(self.master_callback)
            elif (self.event == self.EVENTS.PEER_ACTIVE):
                log.info("connected to backup (master), ready as slave")
                self.state = self.STATES.PASSIVE
                if (self.slave_callback):
                    self.loop.add_callback(self.slave_callback)
            elif (self.event == self.EVENTS.CLIENT_REQUEST):
                if (time.time() >= self.peer_expiry):
                    log.info("request from client, ready as master")
                    self.state = self.STATES.ACTIVE
                    if (self.master_callback):
                        self.loop.add_callback(self.master_callback)
                else:
                    # don't respond to clients yet - we don't know if
                    # the backup is currently Active as a result of
                    # a successful failover
                    accept = False
        elif (self.state == self.STATES.BACKUP):
            # Backup server is waiting for peer to connect
            # Rejects self.EVENTS.CLIENT_REQUEST events in this state
            if (self.event == self.EVENTS.PEER_ACTIVE):
                log.info("connected to primary (master), ready as slave")
                self.state = self.STATES.PASSIVE
                if (self.slave_callback):
                    self.loop.add_callback(self.slave_callback)
            elif (self.event == self.EVENTS.CLIENT_REQUEST):
                accept = False
        elif (self.state == self.STATES.ACTIVE):
            # Server is active
            # Accepts self.EVENTS.CLIENT_REQUEST events in this state
            # The only way out of ACTIVE is death
            if (self.event == self.EVENTS.PEER_ACTIVE):
                # Two masters would mean split-brain
                log.error("fatal error - dual masters, aborting")
                raise FSMError("Dual Masters")
        elif (self.state == self.STATES.PASSIVE):
            # Server is passive
            # self.EVENTS.CLIENT_REQUEST events can trigger failover if peer looks dead
            if (self.event == self.EVENTS.PEER_PRIMARY):
                # Peer is restarting - become active, peer will go passive
                log.info("primary (slave) is restarting, ready as master")
                self.state = self.STATES.ACTIVE
            elif (self.event == self.EVENTS.PEER_BACKUP):
                # Peer is restarting - become active, peer will go passive
                log.info("backup (slave) is restarting, ready as master")
                self.state = self.STATES.ACTIVE
            elif (self.event == self.EVENTS.PEER_PASSIVE):
                # Two passives would mean cluster would be non-responsive
                log.error("fatal error - dual slaves, aborting")
                raise FSMError("Dual slaves")
            elif (self.event == self.EVENTS.CLIENT_REQUEST):
                # Peer becomes master if timeout has passed
                # It's the client request that triggers the failover
                assert (self.peer_expiry > 0)
                if (time.time() >= self.peer_expiry):
                    # If peer is dead, switch to the active state
                    log.info("failover successful, ready as master")
                    self.state = self.STATES.ACTIVE
                else:
                    # If peer is alive, reject connections
                    accept = False
            # Call state change handler if necessary
            if (self.state == self.STATES.ACTIVE and self.master_callback):
                self.loop.add_callback(self.master_callback)
        return accept

    """
    Reactor event handlers
    """

    def send_state(self):
        """Publish our state to peer"""
        self.statepub.send("%d" % self.state)

    def recv_state(self, msg):
        """Receive state from peer, execute finite state machine"""
        state = msg[0]
        if state:
            self.event = int(state)
            self.update_peer_expiry()
        self.execute_fsm()

    def voter_ready(self, msg):
        """Application wants to speak to us, see if it's possible"""
        # If server can accept input now, call appl handler
        self.event = self.EVENTS.CLIENT_REQUEST
        if self.execute_fsm():
            if self._debug:
                log.debug("CLIENT REQUEST")
            self.voter_callback(self.voter_socket, msg)
        else:
            # Message will be ignored
            pass

    def register_voter(self, endpoint, type, handler):
        """Create socket, bind to local endpoint, and register as reader for
        voting. The socket will only be available if the Binary Star state
        machine allows it. Input on the socket will act as a "vote" in the
        Binary Star scheme.  We require exactly one voter per bstar instance.

        handler will always be called with two arguments: (socket,msg)
        where socket is the one we are creating here, and msg is the message
        that triggered the POLLIN event.
        """
        assert self.voter_callback is None

        socket = self.ctx.socket(type)
        socket.bind(endpoint)
        self.voter_socket = socket
        self.voter_callback = handler

        stream = ZMQStream(socket, self.loop)
        stream.on_recv(self.voter_ready)


#from . import clonesrv
#from .bstar import BinaryStar
from .clonesrv import Route, send_single
from .kvmsg import KVMsg
from .zhelpers import dump


class CloneServer(object):
    """ Clone Server object """
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
        #if peer.id in self.peers:
        #    return

        log.info('CloneServer: Adding Peer %s.', peer.uuid)

        uuid = peer.uuid
        self.peers[uuid] = peer

    def remove_peer(self, peer):
        #if peer.id not in self.peers:
        #    return

        log.info('CloneServer: Removing Peer %s.', peer.uuid)

        uuid = peer.uuid
        del self.peers[uuid]

    @property
    def router_endpoint(self):
        return 'tcp://%s:%d' % (self.service_addr, self.port)

    def __init__(self, service_addr='*', port=5556):
        self.service_addr = service_addr
        self.port = port

        if not self.ctx:
            self.ctx = zmq.Context.instance()
        self.loop = IOLoop.instance()

        # Base init
        self.peers = {}
        self.pending = []

        primary = True
        if conf.hostname == 'san0':
            remote_host = 'san1'
        elif conf.hostname == 'san1':
            remote_host = 'san0'
            primary = False
        else:
            remote_host = 'localhost'
        self.remote_host = remote_host
        self.primary = primary

        self.kvmap = {}
        if primary:
            bstar_local_ep = 'tcp://*:5003'
            bstar_remote_ep = 'tcp://%s:5004' % remote_host

            #self.become_master()
        else:
            bstar_local_ep = 'tcp://*:5004'
            bstar_remote_ep = 'tcp://%s:5003' % remote_host
            #self.become_slave()

        # Setup router socket
        #self.router = self.ctx.socket(zmq.ROUTER)
        #self.router.bind(self.router_endpoint)
        #self.router = ZMQStream(self.router)
        #self.router.on_recv_stream(self.handle_snapshot)

        self.bstar = BinaryStar(primary, bstar_local_ep, bstar_remote_ep)
        self.bstar.register_voter(self.router_endpoint,
                                  zmq.ROUTER,
                                  self.handle_snapshot)

        # Set up our clone server sockets
        self.publisher = self.ctx.socket(zmq.PUB)
        self.publisher.bind(self.publisher_endpoint)
        self.publisher = ZMQStream(self.publisher)

        self.collector = self.ctx.socket(zmq.SUB)
        self.collector.setsockopt(zmq.SUBSCRIBE, b'')
        self.collector.bind(self.collector_endpoint)
        self.collector = ZMQStream(self.collector)

        # Register state change handlers
        self.bstar.master_callback = self.become_master
        self.bstar.slave_callback = self.become_slave

        # Register our handlers with reactor
        self.collector.on_recv(self.handle_collect)
        self.flush_callback = PeriodicCallback(self.flush_ttl, 1000)
        self.hugz_callback = PeriodicCallback(self.send_hugz, 1000)

    @property
    def collector_port(self):
        return self.port + 2

    @property
    def publisher_port(self):
        return self.port + 1

    @property
    def collector_endpoint(self):
        return 'tcp://%s:%d' % (self.service_addr, self.collector_port)

    @property
    def publisher_endpoint(self):
        return 'tcp://%s:%d' % (self.service_addr, self.publisher_port)

    def start(self, loop=True):
        log.debug('CloneServer %s start', self)

        # start periodic callbacks
        self.flush_callback.start()
        self.hugz_callback.start()

        # Start bstar reactor until process interrupted
        self.bstar.start(loop=loop)

        #if loop:
        #    self.loop.start()

    def handle_snapshot(self, socket, msg):
        """snapshot requests"""
        log.debug('handle_snapshot: Got msg=%s', msg[1:])

        #if msg[1] == 'IM_SLAVE':
        #    log.debug('Tried to get snapshot from slave.')
        #    return

        if msg[1] != "ICANHAZ?" or len(msg) != 3:
            log.error("bad request, aborting")
            dump(msg)
            self.bstar.loop.stop()
            return

        identity, request = msg[:2]

        if len(msg) >= 3:
            subtree = msg[2]
            # Send state snapshot to client
            route = Route(socket, identity, subtree)

            #if self.kvmap is None:
            #    log.debug('Someone tried to get snapshot from us, but we do not have a kvmap')
            #    socket.send_multipart([identity, 'IM_SLAVE'])
            #    return

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
        self.master = True
        self.slave = False

        # stop receiving subscriber updates while we are master
        #self.subscriber.stop_on_recv()
        #self._handle_subscriber = False
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
        self.kvmap = {}      # clear kvmap

        self.master = False
        self.slave = True

        # start receivinv subscriber updates
        #self.subscriber.on_recv(self.handle_subscriber)
        #self._handle_subscriber = True
        for peer in self.peers.values():
            peer.subscriber.on_recv(peer.subscriber_recv)

    def handle_subscriber(self, peer, msg):
        """Collect updates from peer (master)
        We're always slave when we get these updates
        """
        #if not self._handle_subscriber:
        #    log.debug('handle_subscriber called with msg=%s but we are not slave, so returning', msg)
        #    return

        if self.master:
            log.warn(
                "received subscriber message, but we are master %s", msg)
            return

        # Get state snapshot if necessary
        if self.kvmap is None:
            self.kvmap = {}

            #snapshot = self.ctx.socket(zmq.DEALER)
            #snapshot.linger = 0
            #snapshot.connect("tcp://%s:%i" % (self.remote_host, self.peer))
            #
            #log.info("asking for snapshot from: tcp://%s:%d",
            #         self.remote_host, self.peer)
            #snapshot.send_multipart(["ICANHAZ?", ''])

            snapshot = peer.get_snapshot()

            while True:
                try:
                    kvmsg = KVMsg.recv(snapshot)
                except KeyboardInterrupt:
                    # Interrupted
                    #self.bstar.loop.stop()
                    self.loop.stop()
                    return
                if kvmsg.key == "KTHXBAI":
                    self.sequence = kvmsg.sequence
                    break          # Done
                kvmsg.store(self.kvmap)

            log.info("received snapshot=%d", self.sequence)

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
