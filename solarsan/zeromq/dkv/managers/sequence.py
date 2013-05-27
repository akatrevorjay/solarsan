
from solarsan import logging, LogMixin
logger = logging.getLogger(__name__)
from solarsan.exceptions import NodeNotReadyError

from .base import _BaseManager

import gevent
import gevent.coros
#import zmq.green as zmq

from datetime import datetime
import xworkflows
from collections import deque, Counter, defaultdict


class SequenceSet(object):
    def __init__(self):
        self.values = dict()

    def add(self, v):
        pass


class SequenceManager(_BaseManager, LogMixin, xworkflows.WorkflowEnabled):
    send_seq = 10.0

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        self.seq = Counter(['cur', 'pending'])
        self.counter = Counter()
        self.last_accepted = -1
        self.accepted = defaultdict(deque)
        #self.pending = defaultdict(list)
        self.pending = dict()
        self.pending_seqs = set()
        self.store = defaultdict(deque)

        self.pending_sem = gevent.coros.BoundedSemaphore()

        self._node.seq = self

    def _run(self):
        self.running = True
        while self.running:
            gevent.sleep(self.send_seq)
            if self._node.active:
                # TODO Send out sequence to peers
                self.broadcast('sequence_beat', dict(sequence=self.sequence,
                                                     pending=self.pending))

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

    def allocate_pending(self):
        self.log.debug('Waiting for semaphore')
        self.pending_sem.acquire()
        #with self.pending_sem:
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
            self.pending_sem.release()

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
