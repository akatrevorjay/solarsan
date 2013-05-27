
from solarsan import LogMixin

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

    def set(self, k, v):
        self.store[k] = v

    def pop(self, k, d=None):
        return self.store.pop(k, d)

    #def update(self, data):
    #    self.store.update(data)
