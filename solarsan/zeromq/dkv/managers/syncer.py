
from .base import _BaseManager
#import gevent
from reflex.data import Event


class Syncer(_BaseManager):

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        self.bind(self._on_peer_syncing, 'peer_syncing')

        self._node.greeter = self

    def _on_peer_syncing(self, event, peer):
        self.log.debug('Event: %s peer=%s', event, peer)
        self.peer_sync(peer)

    def peer_sync(self, peer):
        self.log.debug('Syncing %s', peer)

        sync = self._node.kv._export()
        self._debug('sync=%s', sync)
        self.unicast(peer, 'sync', sync)

    def receive_sync(self, peer, sync, *args, **kwargs):
        self.log.debug(
            'Received SYNC from %s: sync=%s args=%s kwargs=%s', peer, sync, args, kwargs)

        if sync.get('data'):
            # TODO Check to see which is more up to date and such
            # TODO Where does this belong?
            sync_seq = sync.get('seq')
            sync_seq_cur = sync_seq.get('cur', 0)

            cur = self._node.seq.seq['cur']

            if sync_seq_cur <= cur:
                self.log.warning('Not syncing as sync_seq_cur=%s <= cur=%s', sync_seq_cur, cur)
            else:
                self.log.info('Syncing as sync_seq_cur=%s > cur=%s', sync_seq_cur, cur)
                self._node.kv._import(sync)

        self.trigger(Event('peer_synced'), peer)
