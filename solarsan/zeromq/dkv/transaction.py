
from solarsan import logging
logger = logging.getLogger(__name__)
import zmq
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback, DelayedCallback
from uuid import uuid4
from datetime import datetime, timedelta


class Transaction(object):
    """Transaction class that handles any updates on their path toward good,
    or evil."""

    uuid = None
    ts = None
    payload = None

    _node = None
    _serialize_attrs = ['uuid', 'ts', 'payload']
    _timeout = timedelta(minutes=1)

    _min_sequence = None
    _votes = None

    """ Base """

    def __init__(self, node, **kwargs):
        self._node = node

        self._votes = {}

        if kwargs:
            self.update(kwargs)

        if not self.uuid:
            self.uuid = uuid4().get_hex()
        if not self.ts:
            self.ts = datetime.now()

        self._set_timeout()

    def update(self, datadict):
        if not datadict:
            return
        for k in self._serialize_attrs:
            setattr(self, k, datadict.get(k))

    """ Tofro dict """

    @classmethod
    def from_dict(cls, agent, datadict):
        return cls(agent, **datadict)

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

    """ Actions """

    def propose(self, cb=None):
        """Flood peers with proposal for us to get stored."""
        #self._node.add_handler('dkv.transaction.propose:%s' % self.uuid, self.on_vote)
        self._node.add_handler('dkv.transaction', self)
        self._node.broadcast('dkv.transaction', 'proposal',
                             self.uuid, self.to_dict())

    def cancel(self, cb=None):
        """Cancel (proposed) transaction."""
        pass

    def commit(self, cb=None):
        """Commit (proposed) transaction."""
        pass

    """ Events """

    def receive_debug(self, *args, **kwargs):
        logger.debug('args=%s; kwargs=%s;', args, kwargs)

    def receive_vote(self, peer, accept, sequence):
        self.receive_debug(accept, sequence)

    def receive_proposal(self, peer, tx_uuid, tx_dict):
        self.receive_debug(peer, tx_uuid, tx_dict)

    #receive_vote = receive_debug
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
