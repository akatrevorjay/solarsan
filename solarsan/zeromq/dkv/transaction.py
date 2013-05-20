
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.exceptions import TransactionError, PeerDidNotAccept, PeerSequenceDidNotMatch
from .manager import _BaseManager

import gevent
import gevent.event

import zmq.green as zmq

from uuid import uuid4
from datetime import datetime, timedelta
#from copy import copy
#import weakref
import xworkflows


class TransactionManager(_BaseManager):
    def __init__(self, node):
        _BaseManager.__init__(self, node)

        #self.pending = weakref.WeakValueDictionary()
        self.pending = dict()

        node.add_handler('dkv.transaction', self)

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
        logger.info('Got transaction proposal from %s: %s', peer, tx_dict)

        tx = ReceiveTransaction.from_dict(self._node, peer, tx_dict)
        tx.link(self._dead_tx)
        #tx.link_exception(self._exception_tx)
        self.append(tx)
        tx.start()

    #def receive_cancel(self, peer, tx_uuid):
    #    if self.has(tx_uuid):
    #        logger.info('Got pending transaction cancellation from %s: %s', peer, tx_uuid)
    #        self.pop(tx_uuid)

    #def receive_commit(self, peer, tx_uuid):
    #    if self.has(tx_uuid):
    #        logger.info('Got pending transaction committance from %s: %s', peer, tx_uuid)
    #        self.pop(tx_uuid)


class _BaseTransaction(gevent.Greenlet):
    """Transaction class that handles any updates on their path toward good,
    or evil."""

    uuid = None
    ts = None
    payload = None
    sender = None
    sequence = None

    _serialize_attrs = ['uuid', 'ts', 'payload', 'sender', 'sequence']
    _timeout = timedelta(seconds=5)

    channel = 'dkv.transaction'

    """ Base """

    def __init__(self, node, **kwargs):
        gevent.Greenlet.__init__(self)
        self.link(self._stop)
        self._node = node
        self.event_done = gevent.event.AsyncResult()

        if kwargs:
            self._update(kwargs)

    def _update(self, datadict):
        if not datadict:
            return
        for k in self._serialize_attrs:
            setattr(self, k, datadict.get(k))

    def _run(self):
        self.running = True

        #gevent.sleep(0.1)

        timeout = gevent.Timeout(seconds=self._timeout.seconds)
        timeout.start()

    @classmethod
    def _stop(cls, self):
        logger.debug('Stopping transaction %s', self.uuid)
        self._remove_handler()
        self.kill()

    """ Tofro dict """

    @classmethod
    def from_dict(cls, node, peer, datadict):
        return cls(node, **datadict)

    def to_dict(self):
        ret = {}
        for k in self._serialize_attrs:
            ret[k] = getattr(self, k, None)
        return ret

    """ Handlers """

    def _add_handler(self):
        self._node.add_handler('dkv.transaction:%s' % self.uuid, self)
        #self.link(self._remove_handler)

    def _remove_handler(self):
        self._node.remove_handler('dkv.transaction:%s' % self.uuid, self)

    """ Actions """

    def allocate_sequence(self):
        # TODO Work with Node and ACTUALLY ALLOCATE a sequence ahead of time!
        # TODO We can then relenquish upon failure or quit without commit, etc
        if not self.sequence:
            self._node.sequence += 1
            self.sequence = self._node.sequence
        return self.sequence

    """ Events """

    def receive_debug(self, *args, **kwargs):
        logger.debug('args=%s; kwargs=%s;', args, kwargs)

    #receive_cancel = receive_debug
    #receive_commit = receive_debug
    #receive_vote = receive_debug


class Transaction(_BaseTransaction, xworkflows.WorkflowEnabled):
    class State(xworkflows.Workflow):
        initial_state = 'init'
        states = (
            ('init',        'Initial state'),
            #('start',       'Start'),
            ('proposal',    'Proposal'),
            ('voting',      'Voting'),
            ('commit',      'Commit'),
            ('cancel',      'Cancelled'),
            ('done',        'Done'),
        )
        transitions = (
            #('start', 'init', 'start'),
            #('propose', 'start', 'proposal'),
            ('propose', 'init', 'proposal'),
            ('receive_vote', 'proposal', 'voting'),
            ('commit', 'voting', 'commit'),
            ('cancel', ('init', 'proposal', 'voting', 'commit'), 'cancel'),
            ('done', ('cancel', 'commit'), 'done'),
        )

    state = State()

    _votes = None

    def __init__(self, node, **kwargs):
        _BaseTransaction.__init__(self, node, **kwargs)
        self.sender = self._node.uuid

        if not self.uuid:
            self.uuid = uuid4().get_hex()
        if not self.ts:
            self.ts = datetime.now()

        self._votes = {}

    def _run(self):
        _BaseTransaction._run(self)

        #gevent.sleep(0.1)

        try:
            #gevent.spawn(self.propose).join()
            self.propose()
            self.event_done.get()
        except gevent.Timeout as e:
            logger.warning('Timeout waiting for votes on transaction %s: %s', self.uuid, e)
            #gevent.spawn(self.cancel).join()
            #raise e

    """ Actions """

    @xworkflows.transition()
    def propose(self):
        """Flood peers with proposal for us to get stored."""
        self.allocate_sequence()
        self._add_handler()
        self._node.broadcast('dkv.transaction', 'proposal',
                             self.uuid, self.to_dict())

    @xworkflows.transition()
    def cancel(self):
        """Cancel (proposed) transaction."""
        self._remove_handler()
        self._node.broadcast('dkv.transaction', 'cancel', self.uuid)
        self.event_done.set()

    @xworkflows.transition()
    def commit(self):
        """Commit (proposed) transaction."""
        logger.info('Committing proposed transaction %s', self.uuid)
        self._remove_handler()
        self._node.broadcast('dkv.transaction', 'commit', self.uuid)
        self.event_done.set()

    #@xworkflows.on_enter_state('commit')
    #def _on_commit(self):
    #    self.event_done.set()

    """ Handlers """

    @xworkflows.transition()
    def receive_vote(self, peer, accept, meta):
        """Sent by each peer that receives a proposal courtesy of self.propose()."""
        logger.info('Transaction %s received vote from %s: %s', self.uuid, peer, accept)
        logger.debug('meta=%s', meta)

        self._votes[peer] = dict(
            accept=accept,
            sequence=meta['sequence'],
            cur_sequence=meta['cur_sequence'],
        )

    @xworkflows.after_transition('receive_vote')
    def after_receive_vote(self, *args):
        logger.debug('args=%s', args)
        if len(self._votes) == len(self._node.peers):
            for k, v in self._votes.iteritems():
                if not v['accept']:
                    logger.warning('Cancelling transaction %s: peer %s did not accept.', self.uuid, k)
                    raise PeerDidNotAccept

                if not v['sequence'] == self.sequence:
                    logger.warning('cancelling transaction %s: peer %s sequence did not match (%s!=%s).',
                                   self.uuid, k, self.sequence, v['sequence'])
                    raise PeerSequenceDidNotMatch
            self.commit()


class ReceiveTransaction(_BaseTransaction, xworkflows.WorkflowEnabled):
    class State(xworkflows.Workflow):
        initial_state = 'proposal'
        states = (
            #('init',        'Initial state'),
            #('start',       'Start'),
            ('proposal',    'Proposal'),
            ('voting',      'Voting'),
            ('commit',      'Commit'),
            ('cancel',      'Cancelled'),
            ('done',        'Done'),
        )
        transitions = (
            #('start', 'init', 'start'),
            #('propose', 'start', 'proposal'),
            #('propose', 'init', 'proposal'),
            ('vote', 'proposal', 'voting'),
            ('commit', 'voting', 'commit'),
            #('cancel', ('init', 'proposal', 'voting', 'commit'), 'cancel'),
            ('cancel', ('proposal', 'voting', 'commit'), 'cancel'),
            ('done', ('cancel', 'commit'), 'done'),
        )

    state = State()

    def __init__(self, node, sender, **kwargs):
        #self.channel = 'dkv.transaction:%s' % self.uuid
        _BaseTransaction.__init__(self, node, **kwargs)
        self.sender = sender
        #self._node.add_handler(

    def _run(self):
        _BaseTransaction._run(self)

        try:
            # TODO COMPARE SEQUENCE TO MAKE SURE ITS OK BEFORE ACCEPTANCE
            #gevent.spawn(self.vote, True).join()
            self.vote()

            self.event_done.get()
        except gevent.Timeout as e:
            logger.warning('Timeout waiting for votes on transaction %s: %s', self.uuid, e)
            #raise e

    """ Actions """

    @xworkflows.transition()
    def vote(self):
        accept = True
        # TODO COMPARE SEQUENCE TO MAKE SURE ITS OK BEFORE ACCEPTANCE
        seq = self.allocate_sequence()
        cur_seq = self._node.sequence

        meta = dict(ts=datetime.now(), sequence=seq, cur_sequence=cur_seq)
        self._node.unicast(self.sender, 'dkv.transaction:%s' % self.uuid, 'vote', accept, meta)

    @xworkflows.transition()
    def cancel(self):
        self.done()
        pass

    @xworkflows.transition()
    def commit(self):
        self.store()
        self.done()

    def store(self):

        pass

    @xworkflows.transition()
    def done(self):
        self.event_done.set()

    #@xworkflows.on_enter_state('commit')
    #def _on_commit(self):
    #    self.event_done.set()

    """ Handlers """

    def receive_cancel(self, peer):
        """Sent by sender to indicate cancellation of this transaction"""
        if peer != self.sender:
            return
        logger.info('Transaction %s received cancel from %s.', self.uuid, peer)
        #gevent.spawn(self.cancel).join()
        self.cancel()

    def receive_commit(self, peer, sequence):
        """Sent by sender to indicate transaction as good to commit."""
        if peer != self.sender:
            return
        logger.info('Transaction %s received commit from %s (sequence=%s).', self.uuid, peer, sequence)
        #gevent.spawn(self.commit).join()
        self.commit()


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
