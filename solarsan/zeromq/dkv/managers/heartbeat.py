
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
