
from solarsan import logging
logger = logging.getLogger(__name__)
#from solarsan.exceptions import TransactionError, PeerDidNotAccept, PeerSequenceDidNotMatch

from .base import _BaseManager
from ..transaction import ReceiveTransaction

import gevent


class TransactionManager(_BaseManager):
    channel = 'dkv.transaction'

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        #self.pending = weakref.WeakValueDictionary()
        self.pending = dict()
        #node.add_handler('dkv.transaction', self)

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
        logger.debug('Dead tx: %s', tx.uuid)
        self.pop(tx)
        del tx

    #def _exception_tx(self, tx):
    #    return self._dead_tx(tx)

    def receive_proposal(self, peer, tx_uuid, tx_dict):
        logger.info('Got transaction proposal from %s: %s', peer, tx_uuid)
        logger.debug('tx_dict=%s', tx_dict)

        tx = ReceiveTransaction.from_dict(self._node, peer, tx_dict)
        #self.append(tx)

        def gen_tx(self, tx):
            tx.link(self._dead_tx)
            #tx.link_exception(self._exception_tx)
            tx.start()

        gevent.spawn(gen_tx, self, tx)
