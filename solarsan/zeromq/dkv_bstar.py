
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

from .util import ZippedPickle
from .beacon import Beacon


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
        self.statesub.setsockopt(zmq.SUBSCRIBE, b'')
        self.statesub.connect(remote)

        # wrap statesub in ZMQStream for event triggers
        self.statesub = ZMQStream(self.statesub)
        self.statesub.on_recv(self.recv_state)

        # setup basic reactor events
        self.heartbeat = PeriodicCallback(
            self.send_state, self.HEARTBEAT)

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

        stream = ZMQStream(socket)
        stream.on_recv(self.voter_ready)
