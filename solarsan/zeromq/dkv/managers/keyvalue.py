
from .base import _BaseManager

from collections import Counter, OrderedDict, defaultdict, deque
from gevent.queue import Queue, LifoQueue, PriorityQueue, JoinableQueue

from ..message import Message


class KeyValueStorageNode(dict):
    pass


class KeyValueNode(_BaseManager):
    store = KeyValueStorageNode()

    def __init__(self, node):
        _BaseManager.__init__(self, node)
