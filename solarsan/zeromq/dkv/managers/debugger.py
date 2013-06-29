
from solarsan import pp
from .base import _BaseManager
from functools import partial


class Debugger(_BaseManager):
    channel = '*'
    ignore_channels = ('Heart', 'Transaction')
    ignore_channel_prefixes = ('Transaction', )

    backdoor = True
    backdoor_listen = '127.0.0.1:0'

    def init(self, node, **kwargs):
        self.log.debug('Debugger init')
        # TODO DEBUG HACKERY
        self.bind(self.on_node_ready, 'node_ready')

    def on_node_ready(self, event, node):
        self.log.debug('Node ready')

    def _run(self):
        # Launch backdoor
        # It's got electrolytes
        if self.backdoor:
            self.backdoor_init()
        _BaseManager._run(self)

    def backdoor_init(self):
        from gevent.backdoor import BackdoorServer

        host, port = self.backdoor_listen.split(':', 1)
        port = int(port)

        self.bd = BackdoorServer((host, port), locals=dict(
            pp=pp,
            n=self._node,
            ms=self._node.managers,
            ps=self._node.peers,
            kv=self._node.kv,
            seq=self._node.seq,
        ))

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

        for c in self.ignore_channel_prefixes:
            if channel.startswith(c):
                return

        # self.log.debug('Debugger [%s] %s: parts=%s kwargs=%s', channel, key,
        # parts, kwargs)
        print ""
        pp([dict(channel=channel, from_peer=peer,
           key=key, parts=parts, kwargs=kwargs)])
        print ""
