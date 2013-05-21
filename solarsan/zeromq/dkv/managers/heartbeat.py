
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.exceptions import NodeNotReadyError

from .base import _BaseManager

import gevent
#import zmq.green as zmq
from datetime import datetime


class HeartbeatSequenceManager(_BaseManager):
    # TODO Lower later
    beat_every_sec = 10.0

    def __init__(self, node):
        self._sequence = -1
        _BaseManager.__init__(self, node)
        self.sequence = 0

        node.seq = self

    def _run(self):
        self.running = True
        while self.running:
            gevent.sleep(self.beat_every_sec)
            self.beat()
            #gevent.spawn(self.bring_out_yer_dead)
            self.bring_out_yer_dead()

    """ Heartbeat """

    @property
    def _meta(self):
        meta = dict()

        if self._check(exception=False):
            meta['cur_sequence'] = self._sequence

        return meta

    def beat(self):
        meta = self._meta
        logger.debug('Sending heartbeat: meta=%s', meta)
        self.broadcast('ping', meta)
        del meta

    def bring_out_yer_dead(self):
        # TODO Check for peer timeouts
        return

    def receive_ping(self, peer, meta):
        logger.info('Heartbeat from %s: meta=%s', peer, meta)

        cur_seq = meta.get('cur_sequence')
        if cur_seq:
            logger.info('cur_seq=%s', cur_seq)
            #if cur_seq != self._sequence

    """ Sequence """

    @property
    def sequence(self):
        self._check()
        return self._sequence

    @sequence.setter
    def sequence(self, value):
        self._sequence = value
        self.ts = datetime.now()

    def _check(self, exception=True):
        if self._sequence < 0:
            if exception:
                raise NodeNotReadyError
            else:
                return False
        return True

    def allocate(self):
        self._check()
        self.sequence += 1
        return self.sequence


