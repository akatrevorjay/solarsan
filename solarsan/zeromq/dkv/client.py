
from solarsan import logging, signals, conf
logger = logging.getLogger(__name__)
from solarsan.pretty import pp
import threading
import time

from solarsan.exceptions import DkvError, DkvTimeoutExceeded
from ..beacon import Beacon
#from ..beacon_greeter import GreeterBeacon
from ..encoders import pipeline
from ..utils import get_address, parse_address
from ..zhelpers import zpipe

import zmq

from .node import Node
from .channel import Channel
from .message import Message, ChannelMessage, MessageContainer
from .transaction import Transaction


"""
Basics
"""


GLOBAL_TIMEOUT = 4000   # If no server replies within this time, abandon request (msecs)
SERVER_TTL = 5.0        # Server considered dead if silent for this long (secs)
SERVER_MAX = 20         # Number of servers we will talk to


"""
Synchronous part, works in our application thread
"""


class DkvClient(object):
    pipe = None         # Pipe through to dkv agent
    agent = None        # agent in a thread
    _subtree = None     # cache of our subtree value
    _default_ttl = 0    # Default TTL
    debug = None
    port = conf.ports.dkv

    class signals:
        on_sub = signals.signal('on_sub')

    def __init__(self, debug=False, discovery=True, connect_localhost=True, subtree=None):
        self.debug = debug
        self.ctx = zmq.Context()
        self.pipe, peer = zpipe(self.ctx)
        # init
        self.active_event = threading.Event()
        self._spawn_agent_thread(peer)
        # connect
        if connect_localhost:
            self.connect(address='tcp://localhost:%d' % self.port)
        if discovery:
            self.connect_via_discovery()
        if subtree:
            self.subtree(subtree)

    def _spawn_agent_thread(self, peer):
        self.agent = DkvAgent(self.ctx, self.active_event)
        self.agent_thread = threading.Thread(target=self.agent.run, args=(peer, ))
        self.agent_thread.daemon = True
        self.agent_thread.start()

    def wait_for_connected(self, timeout=0):
        logger.debug('Waiting for connection..')
        event = self.active_event
        try:
            count = 0
            while timeout == 0 or count < timeout:
                event.wait(timeout=1)
                if event.is_set():
                    return True
                count += 1

            if count > timeout:
                raise DkvTimeoutExceeded('Could not conect to Dkv in specified timeout=%s', timeout)
        except (KeyboardInterrupt, SystemExit):
            raise

    @property
    def subtree(self):
        """ Get property for subtree for snapshot and updates """
        return self._subtree

    @subtree.setter
    def subtree(self, subtree):
        """ Set property for subtree for snapshot and updates.
        Sends [SUBTREE][subtree] to the agent
        """
        self._subtree = None
        self.pipe.send_multipart(["SUBTREE", subtree])
        return self.pipe.recv_multipart()

    """
    Commands
    """

    #def reset(self):
    #    raise NotImplemented()

    #def request_snapshot(self, sequence=-1, peer=None):
    #    raise NotImplemented()

    def connect(self, address=None, host=None, port=conf.ports.dkv, service='', transport='tcp'):
        """ Connect to new server endpoint
        Sends [CONNECT][address] to the agent
        """
        if address is None:
            connect_kwargs = dict(transport=transport, address=address, host=host, port=port, service=service)
            address = get_address(**connect_kwargs)
        self.pipe.send_multipart(['CONNECT', address])
        return self.pipe.recv_multipart()

    def connect_via_discovery(self):
        """ Connect to new server endpoints discovered via beacon
        Sends [CONNECT_DISCOVERY] to the agent
        """
        self.pipe.send_multipart(["CONNECT_DISCOVERY"])
        return self.pipe.recv_multipart()

    def disconnect(self, address, port):
        """ Disconnect to new server endpoint
        Sends [DISCONNECT][address][port] to the agent
        """
        self.pipe.send_multipart(["DISCONNECT", address, str(port)])
        return self.pipe.recv_multipart()

    def set(self, key, value, ttl=_default_ttl, **kwargs):
        """ Set new value in distributed hash table.
        Sends [SET][key][value][ttl][serializer] to the agent
        """
        value = pipeline.dump(value)
        cmd = kwargs.pop('_cmd', 'SET')
        self.pipe.send_multipart([cmd, str(key), str(value), str(ttl)])
        return self.pipe.recv_multipart()

    def get(self, key, default=None, **kwargs):
        """ Lookup value in distributed hash table
        Sends [GET][key] to the agent and waits for a value response
        If there is no dkv available, will eventually return None.
        """
        cmd = kwargs.pop('_cmd', 'GET')
        self.pipe.send_multipart([cmd, str(key)])
        try:
            reply = self.pipe.recv_multipart()
        except KeyboardInterrupt:
            return default
        else:
            value = reply[0]
        value = pipeline.load(value)
        return value or default

    def show(self, key, default=None, **kwargs):
        """ Lookup value in distributed hash table
        Sends [SHOW][key] to the agent and waits for a value response
        If there is no dkv available, will eventually return None.
        """
        kwargs.update(dict(_cmd='SHOW'))
        return self.get(key, default=default, **kwargs)

    def dump_kvmap(self):
        """ Dumps agent's kvmap (which is gets via SHOW KVMAP, then
        loads it via pickle)
        """
        return self.show('KVMAP', pickle=True)

    """
    Magic
    """

    def __getitem__(self, key):
        """ Allows hash-like access.
        eg: dkv['/test']
        """
        return self.get(key)

    def __setitem__(self, key, value):
        """ Allows hash-like sets.
        eg: dkv['/test'] = 'blah'
        """
        return self.set(key, value)


"""
Asynchronous part, works in the background
"""


class DkvServer(object):
    """ Class that represents a single DkvServer """
    address = None          # Server address
    port = None             # Server port
    expiry = 0              # Expires at this time
    requests = 0            # How many snapshot requests made?

    def __init__(self, ctx, subtree, address):
        self.address = address
        connect_kwargs = parse_address(address)
        self.port = int(connect_kwargs['port'])

        self.snapshot = ctx.socket(zmq.DEALER)
        self.snapshot.linger = 0
        connect_kwargs['port'] = conf.ports.dkv
        snapshot_address = get_address(**connect_kwargs)
        logger.debug('Connecting snapshot to %s', snapshot_address)
        self.snapshot.connect(snapshot_address)

        self.subscriber = ctx.socket(zmq.SUB)
        self.subscriber.linger = 0
        self.subscriber.setsockopt(zmq.SUBSCRIBE, subtree)
        connect_kwargs['port'] = conf.ports.dkv_publisher
        subscriber_address = get_address(**connect_kwargs)
        self.subscriber.connect(subscriber_address)


class DkvAgent(object):
    """ Background agent for DkvClient """
    ctx = None              # Own context
    pipe = None             # Socket to talk back to application
    kvmap = None            # Actual key/value dict
    subtree = ''            # Subtree specification, if any
    servers = None          # list of connected Servers
    state = 0               # Current state
    cur_server = 0          # If active, index of server in list
    sequence = 0            # last kvmsg procesed
    publisher = None        # Outgoing updates
    beacon = None
    beacon_cls = Beacon
    debug = None
    active_event = None

    bad_sequence_cnt = 0
    sequence_accept_window = 5

    class STATES:
        """ States we can be in """
        INITIAL = 0         # Before asking server for state
        SYNCING = 1         # Getting state from server
        ACTIVE = 2          # Getting new updates from server

    def __init__(self, ctx, active_event, debug=False):
        self.debug = debug
        self.ctx = ctx
        self.active_event = active_event
        # init
        self.kvmap = {}
        self.subtree = ''
        self.servers = []
        self.state = self.STATES.INITIAL
        # sockets
        self.publisher = self.ctx.socket(zmq.PUSH)
        self.router = self.ctx.socket(zmq.ROUTER)

    def connect(self, address=None):
        kwargs = parse_address(address)
        kwargs['port'] = conf.ports.dkv + 2
        address = get_address(**kwargs)
        #connect_kwargs = parse_address(address)

        if len(self.servers) < SERVER_MAX:
            self.servers.append(DkvServer(
                self.ctx, self.subtree, address))
            logger.debug('Connecting publisher to %s', address)
            #self.publisher.connect("%s:%i" % (address, self.port + 2))
            #self.publisher.connect("%s:%i" % (address, self.port + 2))
            self.publisher.connect(address)
        else:
            logger.error("too many servers (max. %i)", SERVER_MAX)

    def disconnect(self, address=None):
        kwargs = parse_address(address)
        for x, s in enumerate(self.servers):
            logger.debug("Checking if I should disconnect from %s", s)
            if s.address == address and s.port == kwargs['port']:
                logger.info("Disconnecting from '%s:%d'", address, kwargs['port'])
                self.servers.pop(x)
            # Has to be libzmq 3.x for this to work, is it worth leaving?
            self.publisher.disconnect(address)

    def connect_via_discovery(self):
        logger.debug('Starting beacon listener thread in background; will auto-connect to all peers.')
        self.beacon = self.beacon_cls(send_beacon=False)
        self.beacon.on_peer_connected_cb = self._beacon_on_peer_connected
        self.beacon.on_peer_lost_cb = self._beacon_on_peer_lost
        t = self.beacon_thread = threading.Thread(target=self.beacon.start)
        t.daemon = True
        t.start()

    def _beacon_on_peer_connected(self, beacon, peer):
        logger.info('Connecting to server %s', peer.addr)
        address = get_address(transport=peer.transport, host=peer.host)
        self.connect(address)
        #if self.peers:
        #    self.active_event.set()

    def _beacon_on_peer_lost(self, beacon, peer):
        logger.warning('Disconnecting from lost server %s', peer.addr)
        self.disconnect('%s://%s' % (peer.transport, peer.host), conf.ports.dkv)
        if not self.peers:
            self.connected = False

    _state = None

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if self._state != value:
            self._state = value
            if value == self.STATES.ACTIVE:
                self.active_event.set()
            else:
                self.active_event.clear()
            # Reset bad sequence counter
            self.bad_sequence_cnt = 0

    def control_message(self):
        msg = self.pipe.recv_multipart()
        command = msg.pop(0)

        if self.debug:
            logger.debug('cmd=%s msg=%s', command, msg)
            pp(msg)

        if command == "CONNECT":
            self.connect(msg[0])
            self.pipe.send_multipart(['OK'])

        elif command == 'CONNECT_DISCOVERY':
            self.connect_via_discovery()
            self.pipe.send_multipart(['OK'])

        elif command == "DISCONNECT":
            address = msg.pop(0)
            port = int(msg.pop(0))
            self.disconnect(address, port)
            self.pipe.send_multipart(['OK'])

        elif command == "SET":
            key, value, sttl = msg
            ttl = int(sttl)
            value = pipeline.load(value)

            if self.state != self.STATES.ACTIVE:
                self.pipe.send_multipart(['NOT_ACTIVE'])
                return

            # Create and store key-value pair
            kvmsg = Message(0, key=key, body=value)
            kvmsg.store(self.kvmap)
            if ttl:
                kvmsg["ttl"] = ttl
            # Send key-value pair on to server
            kvmsg.send(self.publisher)
            self.pipe.send_multipart(['OK'])

        elif command == "GET":
            key = msg[0]
            value = self.kvmap.get(key)

            if self.state != self.STATES.ACTIVE:
                self.pipe.send_multipart(['NOT_ACTIVE'])
                return

            value = value and value.body or ''
            value = pipeline.dump(value)
            self.pipe.send_multipart([value])

        elif command == "SHOW":
            key = msg[0].upper()
            if key == 'SERVERS':
                self.pipe.send_multipart([
                    str(','.join([x.__repr__() for x in self.servers])), ''])
            elif key == 'SERVER':
                self.pipe.send_multipart([str(self.cur_server), ''])
            elif key == 'SEQ':
                self.pipe.send_multipart([str(self.sequence), ''])
            elif key == 'STATUS':
                self.pipe.send_multipart([str(self.cur_status), ''])
            elif key == 'KVMAP':
                kvmap_s = pipeline.dump(self.kvmap)
                self.pipe.send_multipart([kvmap_s])

    def run(self, pipe):
        """ Asynchronous agent manages server pool and handles request/reply
        dialog when the application asks for it. """
        self.pipe = pipe
        server = None

        while True:
            poller = zmq.Poller()
            poller.register(self.pipe, zmq.POLLIN)
            poll_timer = None
            server_socket = None

            if self.state == self.STATES.INITIAL:
                """In this state we ask the server for a snapshot,
                if we have a server to talk to..."""
                if self.servers:
                    server = self.servers[self.cur_server]
                    logger.debug("Asking for snapshot from %s attempt=%d..",
                                 server.address, server.requests)
                    if (server.requests < 2):
                        server.snapshot.send_multipart(["ICANHAZ?", self.subtree])
                        server.requests += 1
                    server.expiry = time.time() + SERVER_TTL
                    self.state = self.STATES.SYNCING
                    server_socket = server.snapshot

            elif self.state == self.STATES.SYNCING:
                """In this state we read from snapshot and we expect
                the server to respond, else we fail over."""
                server_socket = server.snapshot

            elif self.state == self.STATES.ACTIVE:
                """In this state we read from subscriber and we expect
                the server to give hugz, else we fail over."""
                server_socket = server.subscriber

            if server_socket:
                """we have a second socket to poll"""
                poller.register(server_socket, zmq.POLLIN)

            if server is not None:
                poll_timer = 1e3 * max(0, server.expiry - time.time())

            try:
                # Poll loop
                items = dict(poller.poll(poll_timer))
            except:
                raise  # DEBUG
                break  # Context has been shut down

            if self.pipe in items:
                self.control_message()

            elif server_socket in items:
                #msg = server_socket.recv_multipart()
                #logger.debug('msg=%s', msg)
                #kvmsg = Message.from_msg(msg)
                kvmsg = Message.recv(server_socket)
                #pp(kvmsg.__dict__)

                # Anything from server resets its expiry time
                server.expiry = time.time() + SERVER_TTL

                if self.state == self.STATES.SYNCING:
                    """Store in snapshot until we're finished"""
                    server.requests = 0
                    #logger.debug('Syncing state msg=%s', msg)
                    if kvmsg.key == "KTHXBAI":
                        self.sequence = kvmsg.sequence
                        self.state = self.STATES.ACTIVE
                        logger.info("Synced snapshot=%s from %s", self.sequence, server.address)
                        logger.info("Connected to %s", server.address)
                    else:
                        logger.debug("Syncing update=%s from %s", kvmsg.sequence, server.address)
                        kvmsg.store(self.kvmap)

                elif self.state == self.STATES.ACTIVE:
                    """Discard out-of-sequence updates, incl. hugz"""
                    action = "update" if kvmsg.body else "delete"

                    if kvmsg.sequence < self.sequence \
                       or kvmsg.sequence > self.sequence + self.sequence_accept_window:
                        #logger.warning("Received out of order %s=%d (my sequence is %d) from %s",
                        #               action, kvmsg.sequence, self.sequence, server.address)
                        logger.error("Received %s=%d sequence that is not in the allowed window. "
                                     "(my_sequence=%d, window=%d) from %s",
                                     action, kvmsg.sequence, self.sequence, self.sequence_accept_window,
                                     server.address)
                        self.bad_sequence_cnt += 1

                        if self.bad_sequence_cnt > self.sequence_accept_window:
                            logger.error("Threshold of our of order sequences (>5) was hit. "
                                         "Resetting state so we can resync from scratch.")
                            self.state = self.STATES.INITIAL
                    #else:
                    if kvmsg.sequence > self.sequence:
                        self.sequence = kvmsg.sequence
                        kvmsg.store(self.kvmap)
                        logger.debug("Received %s=%d from %s", action, self.sequence, server.address)

                        # signal
                        if kvmsg.key != 'HUGZ':  # Don't send signals if it's just hugz
                            DkvClient.signals.on_sub.send(
                                kvmsg, key=kvmsg.key, value=kvmsg.body, props=kvmsg.properties)

                        # Reset bad sequence counter
                        self.bad_sequence_cnt = 0

            else:
                """Server has died, failover to next"""
                if self.state == self.STATES.ACTIVE:
                    level = logging.ERROR
                    server_state = 'active'
                else:
                    level = logging.WARNING
                    server_state = 'non-active'
                logger.log(level, "Did not receive heartbeat from %s server at %s; trying next server (if possible, else retrying)",
                           server_state, server.address)
                self.cur_server = (self.cur_server + 1) % len(self.servers)
                self.state = self.STATES.INITIAL


def get_client(debug=True, discovery=True, connect_localhost=True, subtree=None):
    """Create and connect dkv"""
    return DkvClient(debug=debug, discovery=discovery, connect_localhost=connect_localhost, subtree=subtree)


def _test(dkv):
    from .encoders import pickle
    SUBTREE = ''
    dkv['trevorj_yup'] = 'fksdkfjksdf'
    dkv[SUBTREE + 'trevorj'] = 'woot'
    dkv[SUBTREE + 'trevorj-pickle'] = pickle.dumps(
        {'whoa': 'yeah', 'lbh': True})

    logger.debug('SHOW SERVER: %s', dkv.show('SERVER'))
    logger.debug('SHOW SERVERS: %s', dkv.show('SERVERS'))
    logger.debug('SHOW SEQ: %s', dkv.show('SEQ'))


def _test_rand_cache(dkv):
    import random
    # Distribute as key-value message
    key = "%d" % random.randint(1, 10000)
    value = "%d" % random.randint(1, 1000000)
    dkv.set(key, value, random.randint(0, 30))


def main():
    dkv = get_client()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
