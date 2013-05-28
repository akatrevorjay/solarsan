
from solarsan import logging, LogMixin
logger = logging.getLogger(__name__)
from solarsan.exceptions import NodeNotReadyError

from .base import _BaseManager

import gevent
import gevent.coros
# import zmq.green as zmq

from datetime import datetime
import xworkflows
from collections import deque, Counter, defaultdict

import weakref


class SequenceSet(object):

    def __init__(self):
        self.values = dict()

    def add(self, v):
        pass


class SequenceManager(_BaseManager, xworkflows.WorkflowEnabled):
    def __init__(self, node):
        _BaseManager.__init__(self, node)

        # Global
        self.seq = Counter(['cur', 'pending'])
        # Per key
        self.key_seq = Counter()

        # self.last_accepted = -1
        # self.store = defaultdict(deque)
        # self.accepted = defaultdict(deque)
        # self.pending = defaultdict(list)
        # self.pending = dict()
        # self.pending_seqs = set()
        # self.pending_sem = gevent.coros.BoundedSemaphore()

        self.pending = weakref.WeakValueDictionary()

        self._node.seq = self

    tick_length = 1.0

    def _tick(self):
        if self._node.active:
            # TODO Send out sequence to peers
            self.broadcast('sequence_beat', dict(seq=self.current,
                                                 key_seq=self.key_seq,
                                                 #pending=self.pending,
                                                 ))

    def receive_sequence_beat(self, peer, data):
        self.log.debug('Received sequence beat: %s', data)

    """ Machine """

    class State(xworkflows.Workflow):
        initial_state = 'init'
        states = (
            ('init',        'Initial state'),
            #('start',       'Start'),
            ('sync',        'Syncing state'),
            ('active',      'Active state'),
        )
        transitions = (
            #('syncing', 'proposal', 'voting'),
            #('done', 'syncing', 'active'),
            #('cancel', ('init', 'greet', 'sync', 'active'), 'cancel'),
        )

    state = State()

    """ Sequence """

    @property
    def current(self):
        return self.seq['cur']

    """ Allocators """

    def key_current(self, key):
        return self.key_seq[key]

    """ Allocators (global) """

    def pending_tx(self, tx, seq=None):
        ret = None

        if seq:
            if seq <= self.current:
                self.log.warning(
                    'Pending tx sequence=%s is less than current=%s for pending_tx=%s', seq, self.current, tx)
                # TODO Maybe use an exception to signal this with in the props
                # a replacement?
            elif seq in self.pending:
                self.log.warning(
                    'Pending tx sequence=%s is already pending for another pending_tx=%s', seq, self.pending.get(seq))
                # TODO Maybe use an exception to signal this with in the props
                # a replacement?
            else:
                ret = seq

        # if not ret and self._node.active:
        if not ret:
            ret = self.current
            while True:
                ret += 1
                if ret not in self.pending:
                    #self.pending[ret] = weakref.proxy(tx, callback=self.release_pending_tx)
                    self.pending[ret] = tx
                    break

        self.log.debug('Allocated sequence=%s for pending_tx=%s', ret, tx)
        return ret

    def allocate_pending(self):
        self.log.debug('Waiting for semaphore')
        if not self.pending_sem.acquire(timeout=1):
            return 999
        # with self.pending_sem:
        if True:
            self.log.debug('Got semaphore')
            cur = self.current
            ret = cur
            while True:
                ret += 1
                if ret not in self.pending:
                    break
            self.pending[ret] = True
            self.log.debug('Allocated pending seq=%s', ret)
            return ret

    def release_pending(self, seq):
        if seq in self.pending:
            self.log('Releasing pending seq=%s', seq)
            self.pending.pop(seq)
            #self.pending_sem.release()

    """ Events """

    def on_active(self):
        self.log.debug
        self.start()

    def on_syncing(self):
        self.stop()

    on_initial = on_syncing

    """ Sequence """

    def allocate_sequence(self):
        return

    def handle_greeting(self, *args, **kwargs):
        pass

    def handle_beat(self, *args, **kwargs):
        pass

    def handle_proposal(self, *args, **kwargs):
        pass

    def handle_vote(self, *args, **kwargs):
        pass

    def handle_commit(self, *args):
        pass

    @property
    def pending_sequence(self):
        if self.sequence < self._pending_sequence:
            return self._pending_sequence
        else:
            return self.sequence

    @pending_sequence.setter
    def pending_sequence(self, value):
        self._pending_sequence = value
