
from solarsan import LogMixin
from solarsan.exceptions import SolarSanError

from .base import _BaseManager

from collections import Counter, OrderedDict, defaultdict, deque
from gevent.queue import Queue, LifoQueue, PriorityQueue, JoinableQueue

from ..message import Message

from reflex.base import Reactor


class KeyValueStorage(dict):
    pass


class KeyValueManager(_BaseManager, Reactor, LogMixin):
    store = KeyValueStorage()

    def __init__(self, node):
        _BaseManager.__init__(self, node)
        self._node.kv = self

    def get(self, k, d=None):
        return self.store.get(k, d)

    def set(self, k, v, seq):
        # TODO do this better, locking, and this either needs merged into this
        # or moved.
        cur = self._node.seq.current
        if seq <= cur:
            raise SolarSanError("Sequence=%s is not greater than current=%s", seq, cur)
        self.store[k] = v
        self._node.seq.current = seq

    def pop(self, k, d=None):
        return self.store.pop(k, d)

    #def update(self, data):
    #    self.store.update(data)
