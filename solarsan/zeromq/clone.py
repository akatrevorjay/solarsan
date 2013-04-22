"""
clone - client-side Clone Pattern class

Author: Min RK <benjaminrk@gmail.com>
"""

from solarsan import logging
logger = logging.getLogger(__name__)
import threading
import time
from blinker import signal

import zmq

from zhelpers import zpipe
from kvmsg import KVMsg

#from solarsan.pretty import pp
import zmq.utils.jsonapi as json
try:
    import cPickle as pickle
except ImportError:
    import pickle


"""
Basics
"""


# If no server replies within this time, abandon request
GLOBAL_TIMEOUT = 4000    # msecs
# Server considered dead if silent for this long
SERVER_TTL = 5.0     # secs
# Number of servers we will talk to
SERVER_MAX = 2


"""
Synchronous part, works in our application thread
"""


on_sub = signal('on_sub')


class Clone(object):
    ctx = None       # Our Context
    pipe = None      # Pipe through to clone agent
    agent = None     # agent in a thread
    _subtree = None  # cache of our subtree value

    _default_ttl = 0

    class signals:
        on_sub = on_sub

    def __init__(self):
        self.ctx = zmq.Context()
        self.pipe, peer = zpipe(self.ctx)
        self.agent = threading.Thread(
            target=clone_agent, args=(self.ctx, peer))
        self.agent.daemon = True
        self.agent.start()

    """ Clone.subtree is a property, which sets the subtree for snapshot
    # and updates"""

    @property
    def subtree(self):
        return self._subtree

    @subtree.setter
    def subtree(self, subtree):
        """Sends [SUBTREE][subtree] to the agent"""
        self._subtree = None
        self.pipe.send_multipart(["SUBTREE", subtree])

    def connect(self, address, port):
        """Connect to new server endpoint
        Sends [CONNECT][address][port] to the agent
        """
        self.pipe.send_multipart(["CONNECT", address, str(port)])

    def set(self, key, value, ttl=_default_ttl, **kwargs):
        """Set new value in distributed hash table
        Sends [SET][key][value][ttl][serializer] to the agent
        """
        serializer = kwargs.pop('serializer', '')

        allowed_serializers = {}
        if kwargs.pop('pickle', None) is True:
            allowed_serializers['pickle'] = pickle.dumps
            if not serializer:
                serializer = 'pickle'
        if kwargs.pop('json', None) is True:
            allowed_serializers['json'] = json.dumps
            if not serializer:
                serializer = 'json'

        cmd = kwargs.pop('_cmd', 'SET')

        if serializer:
            if serializer in allowed_serializers:
                value = allowed_serializers[serializer](value)
            else:
                raise Exception("Cannot find serializer '%s'" % serializer)

        self.pipe.send_multipart([cmd, key, value, str(ttl), serializer])

    def get(self, key, default=None, **kwargs):
        """Lookup value in distributed hash table
        Sends [GET][key] to the agent and waits for a value response
        If there is no clone available, will eventually return None.
        """
        allowed_serializers = {}
        if kwargs.pop('pickle', None) is True:
            allowed_serializers['pickle'] = pickle.loads
        if kwargs.pop('json', None) is True:
            allowed_serializers['json'] = json.loads
        cmd = kwargs.pop('_cmd', 'GET')

        self.pipe.send_multipart([cmd, key])
        try:
            reply = self.pipe.recv_multipart()
        except KeyboardInterrupt:
            return default
        else:
            value = reply[0]
            serializer = reply[1]

        if serializer in allowed_serializers:
            value = allowed_serializers[serializer](value)

        return value or default

    def show(self, key, default=None, **kwargs):
        """Lookup value in distributed hash table
        Sends [SHOW][key] to the agent and waits for a value response
        If there is no clone available, will eventually return None.
        """
        kwargs.update(dict(_cmd='SHOW'))
        return self.get(key, default=default, **kwargs)

    def dump_kvmap(self):
        """Dumps agent's kvmap (which is gets via SHOW KVMAP, then
        loads it via pickle)"""
        return self.show('kvmap', pickle=True)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)


"""
Asynchronous part, works in the background
"""


class CloneServer(object):
    """Simple class for one server we talk to"""

    address = None          # Server address
    port = None             # Server port
    snapshot = None         # Snapshot socket
    subscriber = None       # Incoming updates
    expiry = 0              # Expires at this time
    requests = 0            # How many snapshot requests made?

    def __init__(self, ctx, address, port, subtree):
        self.address = address
        self.port = port

        self.snapshot = ctx.socket(zmq.DEALER)
        self.snapshot.linger = 0
        self.snapshot.connect("%s:%i" % (address, port))

        self.subscriber = ctx.socket(zmq.SUB)
        self.subscriber.setsockopt(zmq.SUBSCRIBE, subtree)
        self.subscriber.connect("%s:%i" % (address, port + 1))
        self.subscriber.linger = 0


class CloneAgent(object):
    """ Simple class for one background agent """

    class states:
        """ States we can be in """
        initial = 0         # Before asking server for state
        syncing = 1         # Getting state from server
        active = 2          # Getting new updates from server

    ctx = None              # Own context
    pipe = None             # Socket to talk back to application
    kvmap = None            # Actual key/value dict
    subtree = ''            # Subtree specification, if any
    servers = None          # list of connected Servers
    state = 0               # Current state
    cur_server = 0          # If active, index of server in list
    sequence = 0            # last kvmsg procesed
    publisher = None        # Outgoing updates

    def __init__(self, ctx, pipe):
        self.ctx = ctx
        self.pipe = pipe
        self.kvmap = {}
        self.subtree = ''
        self.state = self.states.initial
        self.publisher = ctx.socket(zmq.PUSH)
        self.router = ctx.socket(zmq.ROUTER)
        self.servers = []

    def control_message(self):
        msg = self.pipe.recv_multipart()
        command = msg.pop(0)

        if command == "CONNECT":
            address = msg.pop(0)
            port = int(msg.pop(0))
            if len(self.servers) < SERVER_MAX:
                self.servers.append(CloneServer(
                    self.ctx, address, port, self.subtree))
                self.publisher.connect("%s:%i" % (address, port + 2))
            else:
                logger.error("too many servers (max. %i)", SERVER_MAX)

        elif command == "SET":
            key, value, sttl, serializer = msg
            ttl = int(sttl)

            # Send key-value pair on to server
            kvmsg = KVMsg(0, key=key, body=value)
            kvmsg.store(self.kvmap)
            if ttl:
                kvmsg["ttl"] = ttl
            if serializer:
                kvmsg["serializer"] = serializer
            kvmsg.send(self.publisher)

        elif command == "GET":
            key = msg[0]
            value = self.kvmap.get(key)

            if value:
                body = value.body
                serializer = str(value.properties.get('serializer', ''))
            else:
                body, serializer = ('', '')

            self.pipe.send_multipart([body, serializer])

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
                kvmap_s = pickle.dumps(self.kvmap)
                self.pipe.send_multipart([kvmap_s, 'pickle'])


def clone_agent(ctx, pipe):
    """ Asynchronous agent manages server pool and handles request/reply
    dialog when the application asks for it. """

    agent = CloneAgent(ctx, pipe)
    server = None

    while True:
        poller = zmq.Poller()
        poller.register(agent.pipe, zmq.POLLIN)
        poll_timer = None
        server_socket = None

        if agent.state == agent.states.initial:
            """In this state we ask the server for a snapshot,
            if we have a server to talk to..."""
            if agent.servers:
                server = agent.servers[agent.cur_server]

                logger.debug("waiting for server at %s:%d...",
                             server.address, server.port)

                if (server.requests < 2):
                    server.snapshot.send_multipart(["ICANHAZ?", agent.subtree])
                    server.requests += 1

                server.expiry = time.time() + SERVER_TTL
                agent.state = agent.states.syncing
                server_socket = server.snapshot

        elif agent.state == agent.states.syncing:
            """In this state we read from snapshot and we expect
            the server to respond, else we fail over."""
            server_socket = server.snapshot

        elif agent.state == agent.atates.active:
            """In this state we read from subscriber and we expect
            the server to give hugz, else we fail over."""
            server_socket = server.subscriber

        if server_socket:
            """we have a second socket to poll"""
            poller.register(server_socket, zmq.POLLIN)

        if server is not None:
            poll_timer = 1e3 * max(0, server.expiry - time.time())

        try:
            # Pool loop
            items = dict(poller.poll(poll_timer))
        except:
            raise  # DEBUG
            break  # Context has been shut down

        if agent.pipe in items:
            agent.control_message()

        elif server_socket in items:
            kvmsg = KVMsg.recv(server_socket)

            #if agent.state == agent.states.active and kvmsg.key != 'HUGZ':
            #    # Send out on msg signals
            #    #on_sub.send(kvmsg, key=kvmsg.key, value=kvmsg.body)
            #    on_sub.send(kvmsg, key=kvmsg.key, value=kvmsg.body, props=kvmsg.properties)

            # Anything from server resets its expiry time
            server.expiry = time.time() + SERVER_TTL

            if agent.state == agent.states.syncing:
                """Store in snapshot until we're finished"""
                server.requests = 0
                if kvmsg.key == "KTHXBAI":
                    agent.sequence = kvmsg.sequence
                    agent.state = agent.states.active
                    logger.debug("received from %s:%d snapshot=%d",
                                 server.address, server.port, agent.sequence)
                else:
                    kvmsg.store(agent.kvmap)

                    ## Send on_sub signal out
                    #on_sync_msg.send(kvmsg, key=kvmsg.key, value=kvmsg.body, props=kvmsg.properties)

            elif agent.state == agent.states.active:
                """Discard out-of-sequence updates, incl. hugz"""
                if kvmsg.sequence > agent.sequence:
                    agent.sequence = kvmsg.sequence
                    kvmsg.store(agent.kvmap)
                    action = "update" if kvmsg.body else "delete"

                    logger.debug("received from %s:%d %s=%d",
                                 server.address, server.port, action, agent.sequence)

                    # Don't send signals if it's just hugz
                    if kvmsg.key != 'HUGZ':
                        # Send on_sub signal out
                        on_sub.send(kvmsg, key=kvmsg.key, value=kvmsg.body, props=kvmsg.properties)

        else:
            """Server has died, failover to next"""
            logger.error("server at %s:%d didn't give hugz",
                         server.address, server.port)
            agent.cur_server = (agent.cur_server + 1) % len(agent.servers)
            agent.state = agent.states.initial
