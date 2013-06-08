
#from solarsan.exceptions import TransactionError, PeerDidNotAccept, PeerSequenceDidNotMatch

from .base import _BaseManager
from ..transaction import ReceiveTransaction

import gevent

from collections import deque, Counter


class TransactionManager(_BaseManager):
    def __init__(self, node):
        _BaseManager.__init__(self, node)
        self.pending = dict()

    """ Pending transaction interface """

    def has(self, tx_uuid):
        return tx_uuid in self.pending

    def append(self, tx):
        self.pending[tx.uuid] = tx

    def pop(self, tx_uuid):
        if tx_uuid in self.pending:
            del self.pending[tx_uuid]

    """ Handlers """

    def _dead_tx(self, tx):
        #self.log.debug('Dead tx: %s', tx.uuid)
        self.pop(tx)
        del tx

    #def _exception_tx(self, tx):
    #    return self._dead_tx(tx)

    def receive_proposal(self, peer, tx_uuid, tx_dict):
        self.log.info('Got transaction proposal from %s: %s', peer, tx_uuid)
        self.log.debug('tx_dict=%s', tx_dict)

        tx = ReceiveTransaction.from_dict(self._node, peer, tx_dict)
        #self.append(tx)

        def gen_tx(self, tx):
            tx.link(self._dead_tx)
            #tx.link_exception(self._exception_tx)
            tx.start()

        gevent.spawn(gen_tx, self, tx)
