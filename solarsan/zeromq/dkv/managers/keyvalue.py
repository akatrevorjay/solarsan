
from collections import Counter, OrderedDict, defaultdict, deque
from gevent.queue import Queue, LifoQueue, PriorityQueue, JoinableQueue


class SequenceSet(object):
    def __init__(self):
        self.values = dict()

    def add(self, v):


class SequenceNode:
    def __init__(self):
        self.counter = Counter()
        self.last_accepted = -1
        self.accepted = defaultdict(dequq)
        self.pending = defaultdict(list)

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


class KeyValueStorageNode:
    _store = dict()


class KeyValueNode(_Bas:
    _store = MessageContainer()
    def __init__(self, node):
