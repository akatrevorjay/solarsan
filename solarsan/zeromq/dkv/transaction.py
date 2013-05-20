
from solarsan import logging
logger = logging.getLogger(__name__)
import zmq
from uuid import uuid4
from datetime import datetime, timedelta


class TransactionManager(object):
    def __init__(self, node):
        self._node = node
        self.pending = dict()
        node.add_handler('dkv.transaction', self)

    def receive_proposal(self, peer, tx_uuid, tx_dict):
        logger.info('Got transaction proposal from %s: %s', peer, tx_dict)

        tx = Transaction.from_dict(self._node, tx_dict)
        self.append(tx)

        self.vote(peer, tx_uuid, True)

    def append(self, tx):
        #logger.debug('tx=%s', tx)
        self.pending[tx.uuid] = tx

    def vote(self, peer, tx_uuid, accept):
        tx = self.pending[tx_uuid]
        seq = tx.allocate_sequence()
        cur_seq = self._node.sequence

        meta = dict(ts=datetime.now(), sequence=seq, cur_sequence=cur_seq)
        self._node.unicast(peer, 'dkv.transaction:%s' % tx_uuid, 'vote', accept, meta)

    def receive_cancel(self, peer, tx_uuid):
        if tx_uuid in self.pending:
            logger.info('Got transaction cancellation from %s: %s', peer, tx_uuid)
            self.pending.pop(tx_uuid)

    def receive_commit(self, peer, tx_uuid):
        logger.info('Got transaction committance from %s: %s', peer, tx_uuid)
        if tx_uuid in self.pending:
            #self.pending[tx_uuid].commit()
            self.pending.pop(tx_uuid)

    def receive_debug(self, *args, **kwargs):
        logger.debug('args=%s; kwargs=%s;', args, kwargs)


class Transaction(object):
    """Transaction class that handles any updates on their path toward good,
    or evil."""

    uuid = None
    ts = None
    payload = None

    _node = None
    _serialize_attrs = ['uuid', 'ts', 'payload']
    _timeout = timedelta(minutes=1)

    _sequence = None
    _votes = None

    """ Base """

    def __init__(self, node, **kwargs):
        self._node = node

        if kwargs:
            self.update(kwargs)

        if not self.uuid:
            self.uuid = uuid4().get_hex()
        if not self.ts:
            self.ts = datetime.now()

        self._votes = {}
        self._set_timeout()

    def update(self, datadict):
        if not datadict:
            return
        for k in self._serialize_attrs:
            setattr(self, k, datadict.get(k))

    """ Tofro dict """

    @classmethod
    def from_dict(cls, node, datadict):
        return cls(node, **datadict)

    def to_dict(self):
        ret = {}
        for k in self._serialize_attrs:
            ret[k] = getattr(self, k, None)
        return ret

    """ Timeout """

    def _set_timeout(self):
        self._ttl = self.ts + self._timeout

    def _check_timeout(self):
        return self._ttl < datetime.now()

    """ Handlers """

    def _add_handler(self):
        self._node.add_handler('dkv.transaction:%s' % self.uuid, self)

    def _remove_handler(self):
        self._node.remove_handler('dkv.transaction:%s' % self.uuid, self)

    """ Actions """

    def allocate_sequence(self):
        if not self._sequence:
            self._sequence = self._node.sequence

    proposed = None

    def propose(self):
        """Flood peers with proposal for us to get stored."""
        if not self.proposed:
            self._add_handler()
            self._node.broadcast('dkv.transaction', 'proposal',
                                self.uuid, self.to_dict())
            self.proposed = True

    cancelled = None

    def cancel(self):
        """Cancel (proposed) transaction."""
        if not self.cancelled and self.proposed:
            self._remove_handler()
            self._node.broadcast('dkv.transaction', 'cancel', self.uuid)
            self.cancelled = True

    committed = None

    def commit(self):
        """Commit (proposed) transaction."""
        if not self.committed and not self.cancelled and self.proposed:
            self._remove_handler()
            self._node.broadcast('dkv.transaction', 'commit', self.uuid)
            self.committed = True

    """ Events """

    def receive_debug(self, *args, **kwargs):
        logger.debug('args=%s; kwargs=%s;', args, kwargs)

    def receive_vote(self, peer, accept, meta):
        logger.info('Transaction %s received vote from %s: %s', self.uuid, peer, accept)
        logger.debug('meta=%s', meta)
        self._votes[peer] = dict(
            accept=accept,
            sequence=meta['sequence'],
            cur_sequence=meta['cur_sequence'],
        )

    receive_cancel = receive_debug
    receive_commit = receive_debug


class OldTransaction:
    """ Old Actions """

    def publish(self, sock):
        """Broadcast message to all peers.
        @param sock: publisher socket
        """
        sock.send('TXN_PENDING', zmq.SNDMORE)
        sock.send_json(self.to_dict())

    def pending_recv(cls, data, agent=None):
        self = cls(None, agent=agent, data=data)
        return self

    def vote_recv(self, peer, msg):
        sequence = int(msg[0])

        self.replies[peer.uuid] = sequence
        vote = self.votes.get(sequence, 0)
        self.votes[sequence] = vote + 1

        # TODO Auto-commit here if we have a unanimous vote.
        # ^ need to get the peer count.
        if len(self.votes) >= self._agent.peer_count:
            self.commit(sequence)

    @property
    def max_sequence_vote(self):
        if self.votes:
            return max(set(self.votes.keys()))

    def commit(self, sequence):
        sequence = int(sequence)
        self.publisher.send('TXN_COMMIT', zmq.SNDMORE)
        self.publisher.send(str(sequence))
        self.sequence = sequence
        self.store()

    def commit_recv(self, peer, msg):
        self.sequence = msg[0]
        self.store()

    def store(self):
        #self._agent.kvmap.append(self.payload)
        pass
