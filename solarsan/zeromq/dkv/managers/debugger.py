
from solarsan import logging, LogMeta
logger = logging.getLogger(__name__)
#from solarsan.exceptions import NodeError
from .base import _BaseManager
from functools import partial


class DebuggerManager(_BaseManager):
    __metaclass__ = LogMeta
    channel = '*'

    def __getattribute__(self, key):
        if not hasattr(self, key) and key.startswith('receive_'):
            setattr(self, key, partial(self._receive_debug, key))
        return object.__getattribute__(self, key)

    def _receive_debug(self, key, *parts):
        #self.log.debug('Debugger %s: %s', key, parts, stack=2)
        self.log.debug('Debugger %s: %s', key, parts)
