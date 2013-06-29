
#from solarsan.exceptions import TransactionError, PeerDidNotAccept, PeerSequenceDidNotMatch

from .base import _BaseManager
from ..transaction import ReceiveTransaction

import gevent

#from collections import deque, Counter


class TransactionManager(_BaseManager):
    #debug = True

    def __init__(self, node):
        _BaseManager.__init__(self, node)
        #self.pending = dict()

    """ Pending transaction interface """

    #def has(self, tx_uuid):
    #    return tx_uuid in self.pending

    #def append(self, tx):
    #    self.pending[tx.uuid] = tx

    #def pop(self, tx_uuid):
    #    if tx_uuid in self.pending:
    #        del self.pending[tx_uuid]

    """ Handlers """

    def receive_proposal(self, peer, tx_uuid, tx_dict):
        if not peer.state.is_ready:
            self.log.warning('Got tx proposal while not ready from %s: %s', peer, tx_uuid)
            return

        self.log.info('Got tx proposal from %s: %s', peer, tx_uuid)
        self._debug('tx_dict=%s', tx_dict)

        tx = ReceiveTransaction.from_dict(self._node, peer, tx_dict)
        #self.append(tx)

        gevent.spawn(self._start_tx, tx)

    def _start_tx(self, tx):
        tx.link(self._dead_tx)
        tx.link_exception(self._dead_tx_exception)
        tx.start()

    def _dead_tx(self, tx, exception=False):
        self._debug('Dead tx: %s (exception=%s)', tx.uuid, exception)
        #self.pop(tx)
        #del tx

    def _dead_tx_exception(self, tx):
        return self._dead_tx(tx, exception=True)
