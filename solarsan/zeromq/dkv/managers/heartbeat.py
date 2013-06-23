
#from solarsan.exceptions import NodeNotReadyError
from .base import _BaseManager

import gevent
# import zmq.green as zmq
from datetime import datetime, timedelta
#import xworkflows
#from collections import deque, Counter
from reflex.data import Event


class Heart(_BaseManager):

    debug = False
    tick_length = 1.0
    tick_timeout = 5.0
    tick_wait_until_node_ready = False
    heartbeat_ttl = timedelta(seconds=tick_length * 2)

    # TODO HACK for development (we expect disconnections!)
    neutered = False
    neutered_log = False

    """ Run """

    def _tick(self):
        if self.debug:
            self.log.debug('Tick')

        self.beat()

        if self._node.is_connected:
            self.bring_out_yer_dead()

    """ Helpers """

    @property
    def _meta(self):
        meta = dict()

        # if self._node.active:
        if True:
            meta['sequence'] = dict(current=self._node.seq.current)

        return meta

    def beat(self):
        meta = self._meta
        if self.debug:
            self.log.debug('Sending heartbeat: meta=%s', meta)
        self.broadcast('beat', meta)
        del meta

    def receive_beat(self, peer, meta):
        if self.debug:
            self.log.debug('Heartbeat from %s: meta=%s', peer, meta)
        peer.last_heartbeat_at = datetime.now()
        if getattr(peer, 'receive_beat', None):
            peer.receive_beat(meta)

    def bring_out_yer_dead(self):
        for peer in self._node.peers.values():
            last_heartbeat_at = getattr(peer, 'last_heartbeat_at', None)
            if last_heartbeat_at is None:
                continue
            if last_heartbeat_at + self.heartbeat_ttl < datetime.now():
                if not self.neutered_log:
                    self.log.error(
                        'Have not gotten any heartbeats from %s in too long; marking as dead.', peer)
                # TODO Maybe it's better to mark peer as "disconnected" and not
                # shut it down and remove it for an amount of time? idk,
                # discovery would alleviate that after all. Stick with
                # discovery and removal.
                if not self.neutered:
                    peer.shutdown()


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
        self.log.debug('Received SYNC from %s: sync=%s args=%s kwargs=%s', peer, sync, args, kwargs)

        self.trigger(Event('peer_synced'), peer)
        gevent.spawn_later(2, peer._synced)
