
from solarsan import pp
from .base import _BaseManager
from functools import partial


class Debugger(_BaseManager):
    channel = '*'
    ignore_channels = ('Heart', )

    backdoor = True
    backdoor_listen = '127.0.0.1:0'

    def init(self, node, **kwargs):
        self.bind(self.on_node_ready, 'node_ready')

    def on_node_ready(self, event, node):
        if self.backdoor:
            self.backdoor_init()

    def backdoor_init(self):
        from gevent.backdoor import BackdoorServer

        host, port = self.backdoor_listen.split(':', 1)
        port = int(port)

        self.bd = BackdoorServer((host, port), locals=dict(node=self._node))
        self.bd.start()
        self.log.debug('Initialized backdoor %s', self.bd)

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
