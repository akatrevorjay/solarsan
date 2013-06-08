
from solarsan import pp, LogMixin
from solarsan.exceptions import SolarSanError

from .base import _BaseManager
from ..base import _BaseDict

from collections import Counter, OrderedDict, defaultdict, deque
from gevent.queue import Queue, LifoQueue, PriorityQueue, JoinableQueue

# from ..message import Message

from datetime import datetime


class KeyValueError(SolarSanError):

    """Generic Key Value Error"""


class KeyConflictError(KeyValueError):

    """Conflict between existing data and merge"""


class KeyOutOfSequenceError(KeyConflictError):

    """Sequence out of order"""


def popattr(obj, k, default=None):
    v = getattr(obj, k, default)
    if hasattr(obj, k):
        delattr(obj, k)
    return v


class KeyValueStorage(dict, LogMixin):

    """If memory usage becomes a problem, could only store simpler keyvals on set(),
    and convert back into a Message on get()
    """

    # ^ That may actually make better sense...
    def set_message(self, msg, gseq, sender, ts):
        h = _BaseDict()
        for attr in ('key', 'value', 'uuid'):
            h[attr] = popattr(msg, attr)

        #self.set(h.key, h.value, uuid=h.uuid, gseq=gseq, sender=sender, ts=ts)
        self.set(h.key, h.value)

    def get_message(self, msg):
        pass

    debug = True

    def __init__(self):
        self.kseqs = Counter()
        self.ktimestamps = dict()

    def get_timestamp(self, k):
        return self.ktimestamps[k]

    def get_seq(self, k):
        return self.kseqs[k]

    def check_for_conflict(self, k, seq):
        if seq <= self.kseqs[k]:
            raise KeyOutOfSequenceError

    def set(self, k, v, seq=None, force=False, timestamp=None):
        if seq and not force:
            self.check_for_conflict(k, seq)
        else:
            seq = self.kseqs[k] + 1

        # value
        dict.__setitem__(self, k, v)
        # sequence
        self.kseqs[k] = seq
        # timestamp
        if not timestamp:
            timestamp = datetime.now()
        self.ktimestamps[k] = timestamp

        if self.debug:
            self.log.debug('Set %s=%s (seq=%s, ts=%s)', k, v, seq, timestamp)

    __setitem__ = set

    def remove(self, k):
        dict.__delitem__(self, k)
        if k in self.kseqs:
            del self.kseqs[k]
        if k in self.ktimestamps:
            del self.ktimestamps[k]

    __delitem__ = remove


class KeyValueManager(_BaseManager):
    # debug = False
    debug = True

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
            raise SolarSanError(
                "Sequence=%s is not greater than current=%s", seq, cur)
        self.store[k] = v
        # TODO HACK THIS DOESNT BELONG HERE
        self._node.seq.seq['cur'] = seq

    def pop(self, k, d=None):
        return self.store.pop(k, d)

    # def update(self, data):
    #    self.store.update(data)

    tick_length = 2.5

    def _tick(self):
        if self.debug:
            pp(self.store)
