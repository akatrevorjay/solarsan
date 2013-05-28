
from solarsan import logging, LogMixin
from solarsan.exceptions import NodeNotReadyError

from .base import _BaseManager

import gevent
#import zmq.green as zmq
from datetime import datetime, timedelta
import xworkflows
from collections import deque, Counter


class HeartbeatManager(_BaseManager, LogMixin):

    debug = False
    tick_length = 1.0
    tick_timeout = 5.0
    heartbeat_ttl = timedelta(seconds=tick_length * 2)

    """ Run """

    def _tick_HACK(self):
        if self.debug:
            self.log.debug('Tick')
        #if not self._node.active:
        #    return
        self.beat()
        self.bring_out_yer_dead()

    """ Helpers """

    @property
    def _meta(self):
        meta = dict()

        #if self._node.active:
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
        #peer.receive_beat(meta)

    def bring_out_yer_dead(self):
        for peer in self._node.peers.values():
            last_heartbeat_at = getattr(peer, 'last_heartbeat_at', None)
            if last_heartbeat_at is None:
                continue
            if last_heartbeat_at + self.heartbeat_ttl < datetime.now():
                self.log.error('Have not gotten any heartbeats from %s in too long; marking as dead.', peer)
                peer.shutdown()
