
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from solarsan.core.logger import LogMeta
#from solarsan.exceptions import NodeError

from .base import _BaseManager

#import gevent
from functools import partial


class DebuggerManager(_BaseManager):
    __metaclass__ = LogMeta
    channel = '*'

    def __getattribute__(self, key):
        if not hasattr(self, key) and key.startswith('receive_'):
            setattr(self, key, partial(self._receive_debug, key))
        return object.__getattribute__(self, key)

    def _receive_debug(self, key, *parts):
        logger.debug('Debugger %s: %s', key, parts)
