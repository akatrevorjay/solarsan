
from solarsan import logging, signals, conf
logger = logging.getLogger(__name__)
from solarsan.exceptions import SolarSanError
from solarsan.pretty import pp
import threading
import time
import re

import zmq
from zhelpers import zpipe
from kvmsg import KVMsg

from .beacon import Beacon
#from . import serializers
from .serializers import Pipeline, \
    PickleSerializer, JsonSerializer, MsgPackSerializer, \
    ZippedCompressor, BloscCompressor

pipeline = Pipeline()
pipeline.add(PickleSerializer())
pipeline.add(ZippedCompressor())
#pipeline.add(MsgPackSerializer())
#pipeline.add(BloscCompressor())


"""
Basics
"""


GLOBAL_TIMEOUT = 4000   # If no server replies within this time, abandon request (msecs)
SERVER_TTL = 5.0        # Server considered dead if silent for this long (secs)
SERVER_MAX = 20         # Number of servers we will talk to


"""
Synchronous part, works in our application thread
"""


def get_address(host='localhost', port=None,
                service='', transport='tcp'):
    if not host:
        host = 'localhost'
    if not port:
        port = conf.ports.dkv
    if not transport:
        transport = 'tcp'
    if not service:
        service = ''
    return '%s://%s:%s%s' % (transport, host, port, service)


def parse_address(address):
    m = re.match(r'^(?P<transport>\w+)://(?P<host>[-\w\.]+)(?P<port>:\d+)?(?P<service>/.*)?$', address, re.IGNORECASE)
    if not m:
        raise SolarSanError('Invalid address %s', address)
    ret = m.groupdict()
    ret['port'] = int(ret.get('port') and ret['port'][1:] or conf.ports.dkv)
    return ret


class DkvError(SolarSanError):
    pass


class DkvTimeoutExceeded(DkvError):
    pass


#class DkvNotConnected(DkvError):
#    pass


class Dkv(object):
    ctx = None          # Our Context
    pipe = None         # Pipe through to dkv agent
    agent = None        # agent in a thread
    _subtree = None     # cache of our subtree value
    _default_ttl = 0    # Default TTL
    debug = None

    port = conf.ports.dkv

    class signals:
        on_sub = signals.signal('on_sub')

    def __init__(self, debug=False, discovery=True, connect_localhost=True):
        self.debug = debug
        self.ctx = zmq.Context()
        self.pipe, peer = zpipe(self.ctx)

        self.connected_event = threading.Event()

        self.agent = threading.Thread(target=dkv_agent, args=(self.ctx, peer, self.connected_event))
        self.agent.daemon = True
        self.agent.start()

        if connect_localhost:
            self.connect(address='tcp://localhost:%d' % conf.ports.dkv)

        if discovery:
            self.connect_via_discovery()

    def wait_for_connected(self, timeout=None):
        logger.debug('Waiting for connection..')
        event = self.connected_event
        try:
            count = 0
            while count < timeout:
                event.wait(timeout=1)
                if event.is_set():
                    break
                count += 1
            if count > timeout:
                raise DkvTimeoutExceeded('Could not conect to Dkv in specified timeout=%d', timeout)
        except (KeyboardInterrupt, SystemExit):
            raise
        logger.debug('Connected.')

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

    def connect(self, address=None, host=None, port=conf.ports.dkv, service='', transport='tcp'):
        """ Connect to new server endpoint
        Sends [CONNECT][address] to the agent
        """
        if address is None:
            connect_kwargs = dict(transport=transport, address=address, host=host, port=port, service=service)
            address = get_address(**connect_kwargs)
        #self.pipe.send_multipart(['CONNECT', pipeline.dump(connect_kwargs)])
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

    def reset(self):
        raise NotImplemented()

    def request_snapshot(self, sequence=-1, peer=None):
        raise NotImplemented()

    def set(self, key, value, ttl=_default_ttl, **kwargs):
        """ Set new value in distributed hash table.
        Sends [SET][key][value][ttl][serializer] to the agent
        """
        #serializer = kwargs.pop('serializer', '')
        #serializers = ['pickle', 'json', 'zipped', 'blosck']
        #if kwargs.pop('pickle', None) is True:
        #    if not serializer:
        #        serializer = 'pickle'
        #if kwargs.pop('json', None) is True:
        #    if not serializer:
        #        serializer = 'json'
        #if serializer:
        #    if serializer in allowed_serializers:
        #        value = allowed_serializers[serializer](value)
        #    else:
        #        raise Exception("Cannot find serializer '%s'" % serializer)
        #
        #serializer = pipeline
        #if serializer:
        #    value = serializer.dump(value)

        value = pipeline.dump(value)

        cmd = kwargs.pop('_cmd', 'SET')
        #self.pipe.send_multipart([cmd, key, value, str(ttl), serializer])
        self.pipe.send_multipart([cmd, key, value, str(ttl)])
        return self.pipe.recv_multipart()

    def get(self, key, default=None, **kwargs):
        """ Lookup value in distributed hash table
        Sends [GET][key] to the agent and waits for a value response
        If there is no dkv available, will eventually return None.
        """
        #serializer = kwargs.pop('serializer', '')
        #serializers = ['pickle', 'json', 'zipped', 'blosck']
        #if kwargs.pop('pickle', None) is True:
        #    if not serializer:
        #        serializer = 'pickle'
        #if kwargs.pop('json', None) is True:
        #    if not serializer:
        #        serializer = 'json'
        #allowed_serializers = {}
        #if kwargs.pop('pickle', None) is True:
        #    allowed_serializers['pickle'] = pickle.loads
        #if kwargs.pop('json', None) is True:
        #    allowed_serializers['json'] = json.loads
        #if serializer in allowed_serializers:
        #    value = allowed_serializers[serializer](value)

        cmd = kwargs.pop('_cmd', 'GET')

        self.pipe.send_multipart([cmd, key])
        try:
            reply = self.pipe.recv_multipart()
        except KeyboardInterrupt:
            return default
        else:
            value = reply[0]
            #serializer = reply[1]

        #serializer = pipeline
        #if serializer:
        #    value = serializer.load(value)

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
    """ Simple class for one server we talk to """

    address = None          # Server address
    port = None             # Server port
    expiry = 0              # Expires at this time
    requests = 0            # How many snapshot requests made?

    def __init__(self, ctx, subtree, address):
        self.address = address
        #self.port = port
        connect_kwargs = parse_address(address)
        self.port = int(connect_kwargs['port'])

        self.snapshot = ctx.socket(zmq.DEALER)
        self.snapshot.linger = 0
        #connect_kwargs['service'] = '/dkv/snapshot'
        connect_kwargs['port'] = conf.ports.dkv
        snapshot_address = get_address(**connect_kwargs)
        logger.debug('Connecting snapshot to %s', snapshot_address)
        self.snapshot.connect(snapshot_address)

        self.subscriber = ctx.socket(zmq.SUB)
        self.subscriber.linger = 0
        self.subscriber.setsockopt(zmq.SUBSCRIBE, subtree)
        #connect_kwargs['service'] = '/dkv/pub'
        connect_kwargs['port'] = conf.ports.dkv_publisher
        subscriber_address = get_address(**connect_kwargs)
        self.subscriber.connect(subscriber_address)


class DkvAgent(object):
    """ Simple class for one background agent """

    class STATES:
        """ States we can be in """
        INITIAL = 0         # Before asking server for state
        SYNCING = 1         # Getting state from server
        ACTIVE = 2          # Getting new updates from server

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

    debug = None
    connected_event = None

    def __init__(self, ctx, pipe, connected_event, debug=False):
        self.debug = debug
        self.ctx = ctx

        self.pipe = pipe

        self.kvmap = {}
        self.subtree = ''

        self.state = self.STATES.INITIAL

        self.publisher = ctx.socket(zmq.PUSH)
        self.router = ctx.socket(zmq.ROUTER)

        self.servers = []

        self.connected_event = connected_event

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
        logger.debug('Starting beacon in background; will auto-connect to all peers.')

        self.beacon = Beacon(send_beacon=False)
        self.beacon.on_peer_connected_cb = self._beacon_on_peer_connected
        self.beacon.on_peer_lost_cb = self._beacon_on_peer_lost

        #self.beacon.start(loop=False)
        t = threading.Thread(target=self.beacon.start)
        t.daemon = True
        t.start()

    def _beacon_on_peer_connected(self, beacon, peer):
        logger.debug('Connecting to server %s', peer.addr)
        address = get_address(transport=peer.transport, host=peer.host)
        self.connect(address)

        #self.connected_event.set()

    def _beacon_on_peer_lost(self, beacon, peer):
        logger.warning('Disconnecting from lost server %s', peer.addr)
        self.disconnect('%s://%s' % (peer.transport, peer.host), conf.ports.dkv)

    def control_message(self):
        msg = self.pipe.recv_multipart()
        command = msg.pop(0)

        if self.debug:
            logger.debug('cmd=%s msg=%s', command, msg)
            pp(msg)

        if command == "CONNECT":
            #connect_kwargs = pipeline.load(msg[0])
            #self.connect(**connect_kwargs)
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
            #key, value, sttl, serializer = msg
            key, value, sttl = msg
            ttl = int(sttl)

            value = pipeline.load(value)

            # Send key-value pair on to server
            kvmsg = KVMsg(0, key=key, body=value)
            kvmsg.store(self.kvmap)
            if ttl:
                kvmsg["ttl"] = ttl
            #if serializer:
            #    kvmsg["serializer"] = serializer
            kvmsg.send(self.publisher)
            self.pipe.send_multipart(['OK'])

        elif command == "GET":
            key = msg[0]
            value = self.kvmap.get(key)

            if value:
                body = value.body
                #serializer = str(value.properties.get('serializer', ''))
            else:
                body = ''
                #serializer = ''

            body = pipeline.dump(body)

            #self.pipe.send_multipart([body, serializer])
            self.pipe.send_multipart([body])

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
                #kvmap_s = pickle.dumps(self.kvmap)
                #self.pipe.send_multipart([kvmap_s, 'pickle'])
                kvmap_s = pipeline.dump(self.kvmap)
                self.pipe.send_multipart([kvmap_s])


def dkv_agent(ctx, pipe, connected_event):
    """ Asynchronous agent manages server pool and handles request/reply
    dialog when the application asks for it. """

    agent = DkvAgent(ctx, pipe, connected_event)
    server = None

    while True:
        poller = zmq.Poller()
        poller.register(agent.pipe, zmq.POLLIN)
        poll_timer = None
        server_socket = None

        if agent.state == agent.STATES.INITIAL:
            """In this state we ask the server for a snapshot,
            if we have a server to talk to..."""
            if agent.servers:
                server = agent.servers[agent.cur_server]

                logger.debug("waiting for server at %s...", server.address)

                if (server.requests < 2):
                    server.snapshot.send_multipart(["ICANHAZ?", agent.subtree])
                    server.requests += 1

                server.expiry = time.time() + SERVER_TTL
                agent.state = agent.STATES.SYNCING
                server_socket = server.snapshot

        elif agent.state == agent.STATES.SYNCING:
            """In this state we read from snapshot and we expect
            the server to respond, else we fail over."""
            server_socket = server.snapshot

        elif agent.state == agent.STATES.ACTIVE:
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

        if agent.pipe in items:
            agent.control_message()

        elif server_socket in items:
            msg = server_socket.recv_multipart()
            #logger.debug('server_socket=%s, msg=%s', server_socket, msg)
            #logger.debug('msg=%s', msg)
            #kvmsg = KVMsg.recv(server_socket)
            kvmsg = KVMsg.from_msg(msg)
            #pp(kvmsg.__dict__)

            server.expiry = time.time() + SERVER_TTL    # Anything from server resets its expiry time

            if agent.state == agent.STATES.SYNCING:
                """Store in snapshot until we're finished"""
                server.requests = 0
                if kvmsg.key == "KTHXBAI":
                    agent.sequence = kvmsg.sequence
                    agent.state = agent.STATES.ACTIVE
                    logger.debug("received from %s snapshot=%d",
                                 server.address, agent.sequence)
                    connected_event.set()
                else:
                    kvmsg.store(agent.kvmap)

            elif agent.state == agent.STATES.ACTIVE:
                """Discard out-of-sequence updates, incl. hugz"""
                if kvmsg.sequence > agent.sequence:
                    agent.sequence = kvmsg.sequence
                    kvmsg.store(agent.kvmap)
                    action = "update" if kvmsg.body else "delete"

                    logger.debug("received from %s %s=%d",
                                 server.address, action, agent.sequence)

                    """ Signal """
                    if kvmsg.key != 'HUGZ':  # Don't send signals if it's just hugz
                        Dkv.signals.on_sub.send(kvmsg, key=kvmsg.key, value=kvmsg.body, props=kvmsg.properties)

        else:
            """Server has died, failover to next"""
            logger.error("server at %s didn't give hugz", server.address)
            agent.cur_server = (agent.cur_server + 1) % len(agent.servers)
            agent.state = agent.STATES.INITIAL
