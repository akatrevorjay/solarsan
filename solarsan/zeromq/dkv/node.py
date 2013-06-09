
from solarsan import logging, conf, LogMixin
logger = logging.getLogger(__name__)
from solarsan.exceptions import NodeError, PeerUnknown

import gevent
import gevent.coros
import gevent.event

import zmq.green as zmq
from uuid import uuid4
# from datetime import datetime
# from functools import partial
import weakref
import xworkflows

# from reflex.base import Reactor, Binding, Ruleset
from reflex.data import Event
from reflex.base import Reactor
# from reflex.control import EventManager as _BaseEventManager
from reflex.control import EventManager
from reflex.control import PackageBattery, ReactorBattery, RulesetBattery

# from .message import MessageContainer
# from .channel import Channel
from .encoder import EJSONEncoder
from .peer import Peer

from .managers.heartbeat import Heart
from .managers.sequence import Sequencer
from .managers.transaction import TransactionManager
from .managers.debugger import Debugger
from .managers.keyvalue import KeyValueManager


# class EventManager(_BaseEventManager, LogMixin):
#
#    def __init__(self, *args, **kwargs):
#        _BaseEventManager.__init__(
#            self, self.log.info, self.log.debug, *args, **kwargs)


class _DispatcherMixin(Reactor):

    '''
    Messages are handled by adding instances to the handlers list. The
    first instance that contains a method named 'receive_<message_type>'
    will have that method called. The first argument is always the Peer.
    The remaining positional arguments are filled with the parts of the
    zmq message.
    '''

    debug_handler = False
    debug_channel = False
    debug_dispatch = False
    debug_managers = False

    def __init__(self):
        # event handlers
        self.handlers = dict()

    """ Handlers """

    def add_handler(self, channel_name, handler):
        if not channel_name in self.handlers:
            if self.debug_channel:
                self.log.debug('Add channel: %s', channel_name)
            self.handlers[channel_name] = list()

        if handler not in self.handlers[channel_name]:
            # handler = weakref.proxy(handler)
            if self.debug_handler:
                self.log.debug(
                    'Add handler: %s chan=%s', handler, channel_name)
            self.handlers[channel_name].append(handler)

    def remove_handler(self, channel_name, handler):
        if channel_name in self.handlers:
            if handler in self.handlers[channel_name]:
                if self.debug_handler:
                    self.log.debug(
                        'Remove handler: %s chan=%s', handler, channel_name)
                self.handlers[channel_name].remove(handler)
            if not self.handlers[channel_name]:
                if self.debug_channel:
                    self.log.debug('Remove channel: %s', channel_name)
                self.handlers.pop(channel_name)

    def init_managers(self, *managers):
        """ Managers """

        self.managers = dict()

        if not managers:
            managers = []
        managers += self._default_managers

        for cls in managers:
            if self.debug_managers:
                self.log.debug('Loading manager %s', cls)
            cls(self)

    """ Managers """

    def add_manager(self, manager, name=None):
        if not name:
            # name = repr(manager)
            name = manager.__class__.__name__
            name = name.rsplit('Manager', 1)[0]

        if not name in self.managers:
            self.managers[name] = manager
            if self.started:
                self._start_managers(manager)

    def remove_manager(self, manager, name=None):
        if not name:
            name = manager.__class__.__name__
            name = name.rsplit('Manager', 1)[0]

        if name in self.managers:
            del self.managers[name]

    def _start_managers(self, *managers):
        if not managers:
            managers = self.managers.iteritems()
        for k, v in managers:
            if not getattr(v, 'started', None):
                # if self.debug_managers:
                self.log.debug('Spawning Manager %s: %s', k, v)
                v.link(self.remove_manager)
                v.start()
        gevent.sleep(0)

    def _dispatch(self, from_peer, channel_name, message_type, parts, _looped=None):
        peer = self.get_peer(from_peer, exception=None)
        if peer is None:
            self.log.error(
                'Ignoring dispatch call with an unknown peer: %s', from_peer)
            return

        if self.debug_dispatch and not _looped:
            self.log.debug('Dispatch: peer=%s chan=%s msg_type=%s parts=%s',
                           peer, channel_name, message_type, parts)

        kwargs = dict()

        # Handle wildcard channel
        if channel_name != '*':
            self._dispatch(
                peer, '*', message_type, parts, _looped=channel_name)
        else:
            kwargs['channel'] = _looped

        # Dispatch
        handlers = self.handlers.get(channel_name, None)
        if handlers:
            for h in handlers:
                f = getattr(h, 'receive_' + message_type, None)
                if f:
                    # if self.debug_dispatch:
                    #    self.log.debug('Dispatching to: %s', h)
                    gevent.spawn(f, peer, *parts, **kwargs)
                    # break


class _PeersMixin:
    debug_peers = False
    peers = None

    def __init__(self):
        # peers
        self.peers = dict()

        # bind peer_ready event
        self.bind(self._on_peer_ready, 'peer_ready')

    def connect(self):
        self.log.info('Connecting..')

        peers = self.peers.copy()
        gs = []
        for uuid, peer in peers.iteritems():
            g = gevent.spawn(self.connect_peer, peer)
            gs.append(g)
        for g in gs:
            # g.join(timeout=10)
            g.join()

    def add_peer(self, peer):
        if self.debug_peers:
            self.log.debug('Adding peer: %s', peer)
        uuid = str(peer.uuid)

        # TODO I don't believe this is the correct way of handling this.
        # We should just ensure the connection is still alive.
        # Then again, this is really just a corner case.
        #
        # If we already have uuid, replace it
        if peer.uuid in self.peers:
            self.peers[uuid].shutdown()

        self.peers[uuid] = peer
        self.connect_peer(peer)

    def remove_peer(self, peer):
        self.log.info('Removing peer: %s', peer)
        uuid = str(peer.uuid)
        if uuid in self.peers:
            del self.peers[uuid]

    def connect_peer(self, peer):
        """Connects to peer"""

        self.log.info('Connecting to peer: %s', peer)

        # Connect router
        self.rtr.connect(peer.rtr_addr)

        # Add subscriber socket to our sock repertoire
        self._add_sock(peer.sub, self._on_sub_received)

        # Set timeout
        # t = gevent.Timeout(seconds=30)

        # Greet peer
        # >> If timeout hits here, try again <<
        # Upon greet received, connect to pub_addr specified

        gevent.sleep(0.1)

    def get_peer(self, peer_or_uuid, exception=PeerUnknown):
        peer = None
        if not isinstance(peer_or_uuid, Peer):
            peer = self.peers.get(peer_or_uuid)
        else:
            peer = peer_or_uuid
        if not peer and exception:
            raise exception(peer_or_uuid)
        return peer

    def _on_peer_ready(self, event, peer):
        # TODO THIS IS SPAGHETTI BULLSHIT. CLEAN IT UP, NAMES IN PARTICULAR!
        self.log.debug('Peer ready: %s', peer)
        self.log.debug('Event: %s', event)
        if not self.is_ready:
            self.synced()


class _CommunicationsMixin:

    _ctx = None
    _poller = None

    def __init__(self, encoder):
        # encoder
        self.encoder = encoder

        """ Sockets """

        # 0MQ init
        if not getattr(self, '_ctx', None):
            self._ctx = zmq.Context.instance()
        if not getattr(self, '_poller', None):
            self._poller = zmq.Poller()

        # dict of sockets to poll
        self._socks = dict()

        # router socket
        rtr = self.rtr = self._ctx.socket(zmq.ROUTER)
        rtr.setsockopt(zmq.IDENTITY, self.uuid)
        self._add_sock(rtr, self._on_rtr_received)

        # publisher socket
        self.pub = self._ctx.socket(zmq.PUB)

        for sock in (self.rtr, self.pub):
            sock.linger = 0

    def broadcast(self, channel_name, message_type, *parts):
        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]
        l = ['solarsan', str(channel_name)]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))
        self.pub.send_multipart(l)

    def unicast(self, peer, channel_name, message_type, *parts):
        peer = self.get_peer(peer)

        if peer.uuid == self.uuid:
            self.dispatch(
                self.uuid, channel_name, message_type, parts)
            return

        if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
            parts = parts[0]

        l = [str(peer.uuid), str(channel_name)]
        l.extend(self.encoder.encode(self.uuid, message_type, parts))

        self.rtr.send_multipart(l)

    def _on_rtr_received(self, raw_parts):
        # discard source address. We'll use the one embedded in the message
        # for consistency
        # self.log.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    def _on_sub_received(self, raw_parts):
        # discard the message header. Can address targeted subscriptions
        # later
        # self.log.info('raw_parts=%s', raw_parts)
        channel_name = raw_parts[1]
        from_uuid, message_type, parts = self.encoder.decode(raw_parts[2:])
        self._dispatch(from_uuid, channel_name, message_type, parts)

    # def receive_discovery(self, peer_uuid, peer_router):
    #    """Connects to discovered peer"""
    #    self.connect(peer_uuid, router=peer_router)


class Node(gevent.Greenlet, xworkflows.WorkflowEnabled,
           _DispatcherMixin, _PeersMixin, _CommunicationsMixin,
           LogMixin):

    debug = True

    _default_encoder_cls = EJSONEncoder
    _default_managers = [Heart, Sequencer, TransactionManager, KeyValueManager]
    if debug:
        _default_managers += [Debugger]

    events = None
    uuid = None

    event_syncing = None
    event_ready = None

    def __init__(self, uuid=None, encoder=_default_encoder_cls(),
                 events=EventManager(stdout=logger.warning, stddebug=logger.debug)):
        gevent.Greenlet.__init__(self)
        self.events = events
        Reactor.__init__(self, self.events, uuid=uuid, encoder=encoder)

    def init(self, *args, **kwargs):
        # uuid
        uuid = kwargs.get('uuid')
        if not uuid:
            uuid = uuid4().get_hex()
        self.uuid = str(uuid)

        self.event_syncing = gevent.event.AsyncResult()
        self.event_ready = gevent.event.AsyncResult()

        _PeersMixin.__init__(self)

        _CommunicationsMixin.__init__(self, kwargs['encoder'])

        _DispatcherMixin.__init__(self)
        managers = kwargs.get('managers', [])
        self.init_managers(*managers)

    def __repr__(self):
        return "<%s uuid='%s'>" % (self.__class__.__name__, self.uuid)

    """ State """

    class State(xworkflows.Workflow):
        initial_state = 'init'
        states = (
            ('init',            'Initial state'),
            ('starting',        'Starting state'),
            ('syncing',         'Syncing state'),
            ('ready',           'Ready state'),
        )
        transitions = (
            ('start', 'init', 'starting'),
            ('bind_listeners', 'starting', 'syncing'),
            ('synced', 'syncing', 'ready'),
        )

    state = State()

    @property
    def is_ready(self):
        return self.state.is_ready
        # return self.event_ready.is_set()

    @property
    def is_connected(self):
        # TODO This is stupid, make connected an event of some sort or better
        # yet, KEEP TRACK OF READY PEERS DIFFERENTLY FROM SAY CONNECTING ONES
        return self.state.is_ready or self.state.is_syncing

    def wait_until_syncing(self, timeout=None):
        return self.event_syncing.wait(timeout=timeout)

    @xworkflows.on_enter_state('syncing')
    def _on_syncing(self, *args):
        self.trigger(Event('node_syncing'), self)
        self.event_syncing.set()

    @xworkflows.on_enter_state('ready')
    def _on_ready(self, *args):
        self.trigger(Event('node_ready'), self)
        self.event_ready.set()

    def wait_until_ready(self, timeout=None):
        return self.event_ready.wait(timeout=timeout)

    """ Run """

    @xworkflows.transition()
    def start(self):
        return gevent.Greenlet.start(self)

    def _run(self):
        self.running = True
        self._start_managers()

        while self.running:
            socks = dict(self._poller.poll(timeout=0))
            if socks:
                # self.log.debug('socks=%s', socks)
                for s, sv in self._socks.iteritems():
                    cb, flags = sv
                    if s in socks and socks[s] == flags:
                        cb(s.recv_multipart())
            gevent.sleep(0.1)

    def _add_sock(self, sock, cb, flags=zmq.POLLIN):
        self._poller.register(sock, flags)
        self._socks[sock] = (cb, flags)

    @xworkflows.transition()
    def bind_listeners(self, rtr_addr, pub_addr):
        self.rtr.bind(rtr_addr)
        self.pub.bind(pub_addr)

        gevent.sleep(0.1)

    def shutdown(self):
        self.log.info('Shutting down Node: %s', self)

        # peer sub socks
        for s, sv in self._socks.iteritems():
            s.close()

        # extraneous socks
        for attr in ('pub', ):
            s = getattr(self, attr, None)
            if s is not None:
                s.close()
