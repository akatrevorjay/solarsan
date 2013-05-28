
from solarsan import logging, LogMixin
logger = logging.getLogger(__name__)
from solarsan.exceptions import TransactionError, PeerDidNotAccept, PeerSequenceDidNotMatch

import gevent
import gevent.event
import zmq.green as zmq
from uuid import uuid4
from datetime import datetime, timedelta
# from copy import copy
# import weakref
import xworkflows


class _BaseTransaction(gevent.Greenlet, LogMixin):

    """Transaction class that handles any updates on their path toward good,
    or evil."""

    ts = None
    payload = None
    sender = None
    sequence = None

    _serialize_attrs = ['uuid', 'ts', 'payload', 'sender', 'sequence']
    _timeout = timedelta(seconds=5)

    channel = 'Transaction'

    """ Base """

    def __init__(self, node, **kwargs):
        gevent.Greenlet.__init__(self)
        self.link(self._stop)
        self._node = node
        self.event_done = gevent.event.AsyncResult()

        if kwargs:
            self._update(kwargs)

    def _post_init(self):
        self._add_handler()
        #return

    def _update(self, datadict):
        if not datadict:
            return
        for k in self._serialize_attrs:
            setattr(self, k, datadict.get(k))

    def _run(self):
        self.running = True
        self._run_timeout = gevent.Timeout(seconds=self._timeout.seconds)
        self._run_timeout.start()

    @classmethod
    def _stop(cls, self):
        # self.log.debug('Stopping tx %s', self)
        if self.sequence:
            self._node.seq.release_pending(self.sequence)
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
        self.channel_tx = '%s:%s' % (self.channel, self.uuid)
        self._node.add_handler(self.channel_tx, self)
        self._node.add_handler(self.channel, self)

    def _remove_handler(self):
        self._node.remove_handler(self.channel_tx, self)
        self._node.remove_handler(self.channel, self)

    """ Actions """

    def store(self):
        # self.log.info('Storing tx %s', self)

        payload = self.payload
        self._node.kv.set(payload['key'], payload, seq=self.sequence)

        self.log.debug('Stored tx %s', self)

    """ Helpers """

    def broadcast(self, message_type, *parts, **kwargs):
        channel = kwargs.pop('channel', self.channel)
        return self._node.broadcast(channel, message_type, *parts)

    def unicast(self, peer, message_type, *parts, **kwargs):
        channel = kwargs.pop('channel', self.channel)
        return self._node.unicast(peer, channel, message_type, *parts)

    """ Events """

    def receive_debug(self, *args, **kwargs):
        self.log.debug('args=%s; kwargs=%s;', args, kwargs)


class Transaction(_BaseTransaction, xworkflows.WorkflowEnabled, LogMixin):

    class State(xworkflows.Workflow):
        initial_state = 'init'
        states = (
            ('init',        'Initial state'),
            #('start',       'Start'),
            ('proposal',    'Proposal'),
            ('voting',      'Voting'),
            ('commit',      'Commit'),
            ('abort',      'Abortled'),
            ('done',        'Done'),
        )
        transitions = (
            #('start', 'init', 'start'),
            #('propose', 'start', 'proposal'),
            ('propose', 'init', 'proposal'),
            #('receive_vote', 'proposal', 'voting'),
            ('voting', 'proposal', 'voting'),
            ('commit', 'voting', 'commit'),
            ('abort', ('init', 'proposal', 'voting', 'commit'), 'abort'),
            ('done', ('abort', 'commit'), 'done'),
        )

    state = State()

    _votes = None

    def __init__(self, node, **kwargs):
        _BaseTransaction.__init__(self, node, **kwargs)
        self.sender = str(self._node.uuid)

        if not self.uuid:
            self.uuid = uuid4().get_hex()
        if not self.ts:
            self.ts = datetime.now()

        self._votes = {}

        self._post_init()

    def _run(self):
        _BaseTransaction._run(self)

        try:
            # gevent.spawn(self.propose).join()
            self.propose()
            self.event_done.get()
            self._run_timeout.cancel()
        except gevent.Timeout as e:
            self.log.error(
                'Timeout waiting for votes on tx %s: %s', self, e)
            self.abort()

    """ Actions """

    @xworkflows.transition()
    def propose(self):
        """Flood peers with proposal for us to get stored."""
        if not self.sequence:
            # self.sequence = self._node.seq.allocate_pending()
            self.sequence = self._node.seq.pending_tx(self)

        self.broadcast('proposal', self.uuid, self.to_dict())

    @xworkflows.transition()
    def abort(self):
        """Abort (proposed) tx."""
        self.log.warning('Aborting tx %s', self)
        self.broadcast('abort', channel=self.channel_tx)
        #self.done()

    @xworkflows.transition()
    def commit(self):
        """Commit (proposed) tx."""
        self.log.info('Committing tx %s', self)
        self.broadcast('commit', self.sequence, channel=self.channel_tx)
        self.store()

    @xworkflows.on_enter_state('commit')
    def enter_commit(self, r):
        self.done()

    #@xworkflows.transition()
    @xworkflows.on_enter_state('done')
    def enter_done(self, r):
        #self.log.debug('Done with tx %s.', self.uuid)
        self.event_done.set()

    """ Handlers """

    #@xworkflows.transition()
    def receive_vote(self, peer, accept, meta):
        self.voting()

        """Sent by each peer that receives a proposal courtesy of self.propose()."""
        self.log.info(
            'Received vote on %s from %s: accept=%s meta=%s',
            self, peer, accept, meta)
        # self.log.debug('meta=%s', meta)

        self._votes[peer] = dict(
            accept=accept,
            sequence=meta['sequence'],
            cur_sequence=meta['cur_sequence'],
        )

        self.check_if_done()

    #@xworkflows.after_transition('receive_vote')
    def check_if_done(self, *args):
        # self.log.debug('args=%s', args)
        if len(self._votes) == len(self._node.peers):
            for k, v in self._votes.iteritems():
                if not v['accept']:
                    self.log.error(
                        'Aborting tx %s: peer %s did not accept.', self, k)
                    raise PeerDidNotAccept

                if not v['sequence'] == self.sequence:
                    self.log.error(
                        'Aborting tx %s: peer %s sequence did not match (%s!=%s).',
                        self, k, self.sequence, v['sequence'])
                    raise PeerSequenceDidNotMatch
            self.commit()

    def __repr__(self):
        return "<%s uuid='%s'>" % (self.__class__.__name__, self.uuid)


class ReceiveTransaction(_BaseTransaction, xworkflows.WorkflowEnabled, LogMixin):

    class State(xworkflows.Workflow):
        #initial_state = 'prepare'
        initial_state = 'proposal'
        states = (
            #('prepare',     'Prepare'),
            ('proposal',    'Proposal'),
            ('voting',      'Voting'),
            ('commit',      'Commit'),
            ('abort',      'Abortled'),
            ('done',        'Done'),
        )
        transitions = (
            ('vote', 'proposal', 'voting'),
            ('commit', 'voting', 'commit'),
            ('abort', ('proposal', 'voting', 'commit'), 'abort'),
            ('done', ('abort', 'commit'), 'done'),
        )

    state = State()

    def __init__(self, node, sender, **kwargs):
        _BaseTransaction.__init__(self, node, **kwargs)
        self.sender = sender

        self._post_init()

    def _run(self):
        _BaseTransaction._run(self)

        try:
            gevent.spawn(self.vote)
            self.event_done.get()
            self._run_timeout.cancel()
        except gevent.Timeout as e:
            self.log.error(
                'Timeout waiting for votes on tx %s: %s', self, e)

    """ Actions """

    @property
    def _vote_meta(self):
        # self.sequence = self._node.seq.allocate_pending()
        seq = self._node.seq.pending_tx(self, self.sequence)

        cur_seq = self._node.seq.current
        return dict(ts=datetime.now(), sequence=seq, cur_sequence=cur_seq)

    @xworkflows.transition()
    def vote(self):
        # TODO COMPARE SEQUENCE TO MAKE SURE ITS OK BEFORE ACCEPTANCE
        accept = self.sequence > self._node.seq.current
        meta = self._vote_meta
        self.log.debug(
            'Sending vote for tx %s: accept=%s meta=%s', self, accept, meta)
        #self.unicast(self.sender, 'vote', self.uuid, accept, meta, channel=self.channel_tx)
        self.broadcast('vote', accept, meta, channel=self.channel_tx)

    @xworkflows.transition()
    def abort(self):
        self.log.warning('Aborting tx %s from %s (sequence=%s).',
                         self, self.sender, self.sequence)
        self.done()

    @xworkflows.transition()
    def commit(self):
        self.log.info('Committing tx %s from %s (sequence=%s).',
                      self, self.sender, self.sequence)
        self.store()

    #@xworkflows.after_transition('commit')
    @xworkflows.on_enter_state('commit')
    def enter_commit(self, r):
        self.done()

    #@xworkflows.transition()
    @xworkflows.on_enter_state('done')
    def enter_done(self, r):
        # self.log.debug('Done with tx %s.', self)
        self.event_done.set()

    """ Handlers """

    def receive_abort(self, peer):
        """Sent by sender to indicate abortlation of this tx"""
        if peer != self.sender:
            return
        # self.log.info('Transaction %s received abort from %s.', self, peer)
        # gevent.spawn(self.abort).join()
        self.abort()

    def receive_commit(self, peer, sequence):
        """Sent by sender to indicate tx as good to commit."""
        if str(peer.uuid) != self.sender:
            self.log.error('Received commit from sender %s that does not match proposer %s', peer, self.sender)
            return
        # self.log.info('Transaction %s received commit from %s (sequence=%s).', self, peer, sequence)
        # gevent.spawn(self.commit).join()
        self.commit()


'''
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
        # self._agent.kvmap.append(self.payload)
        pass
'''
