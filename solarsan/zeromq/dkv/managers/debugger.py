
from solarsan import pp
from .base import _BaseManager
from functools import partial


class DebuggerManager(_BaseManager):
    channel = '*'

    ignore_channels = ('Heartbeat', )

    def __getattribute__(self, key):
        if not hasattr(self, key) and key.startswith('receive_'):
            setattr(self, key, partial(self._receive_debug, key))
        return object.__getattribute__(self, key)

    def _receive_debug(self, key, peer, *parts, **kwargs):
        key = key.split('receive_', 1)[1]
        channel = kwargs.pop('channel')

        if channel in self.ignore_channels:
            return

        #self.log.debug('Debugger [%s] %s: parts=%s kwargs=%s', channel, key, parts, kwargs)
        print ""
        pp([dict(channel=channel, from_peer=peer, key=key, parts=parts, kwargs=kwargs)])
        print ""
