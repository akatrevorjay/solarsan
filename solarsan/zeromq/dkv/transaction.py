
from solarsan import LogMixin
from solarsan.exceptions import TransactionError, PeerDidNotAccept, PeerSequenceDidNotMatch

import gevent
import gevent.event
from uuid import uuid4
from datetime import datetime, timedelta
# from copy import copy
# import weakref
import xworkflows
from .message import Message
from .mixins import DebugLogMixin


class _BaseTransaction(gevent.Greenlet, DebugLogMixin):

    """Transaction class that handles any updates on their path toward good,
    or evil."""

    ts = None
    payload = None
    sender = None
    sequence = None

    _serialize_attrs = ['uuid', 'ts', 'payload', 'sender', 'sequence']
    _timeout = timedelta(seconds=1)

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
        if not isinstance(self.payload, Message):
            self.payload = Message(**self.payload)

    def _run(self):
        self.running = True
        self._run_timeout = gevent.Timeout(seconds=self._timeout.seconds)
        self._run_timeout.start()

        try:
            gevent.spawn(self._main).join()

            self.event_done.get()
            self._run_timeout.cancel()
        except gevent.Timeout as e:
            gevent.spawn(self._on_timeout, e).join()

    def _main(self):
        pass

    def _on_timeout(self, e):
        pass

    @classmethod
    def _stop(cls, self):
        self._debug('Stopping tx %s', self)
        if self.sequence:
            self._node.seq.release_pending(self.sequence)
        self._remove_handler()
        #self.kill()

    def done(self):
        #self._debug('Done with tx %s.', self)
        self.event_done.set()

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

        self._debug('Stored tx %s', self)

    """ Helpers """

    def broadcast(self, message_type, *parts, **kwargs):
        channel = kwargs.pop('channel', self.channel)
        return self._node.broadcast(channel, message_type, *parts)

    def unicast(self, peer, message_type, *parts, **kwargs):
        channel = kwargs.pop('channel', self.channel)
        return self._node.unicast(peer, channel, message_type, *parts)

    """ Events """

    def receive_debug(self, *args, **kwargs):
        self._debug('args=%s; kwargs=%s;', args, kwargs)


class Transaction(_BaseTransaction, xworkflows.WorkflowEnabled, LogMixin):

    class State(xworkflows.Workflow):
        #initial_state = 'init'
        initial_state = 'proposal'
        states = (
            ('proposal',    'Proposal'),
            ('voting',      'Voting'),
            ('commit',      'Commit'),
            ('abort',       'Aborted'),
        )
        transitions = (
            ('voting', ('proposal', 'voting'), 'voting'),
            # Careful when in proposal state, thats only valid if peerless
            ('commit', ('proposal', 'voting'), 'commit'),
            ('abort', ('proposal', 'voting', 'commit'), 'abort'),
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

    def __repr__(self):
        return "<%s uuid='%s' is_peerless=%s>" % (self.__class__.__name__, getattr(self, 'uuid', None), self.is_peerless)

    def _main(self):
        return self.propose()

    def _on_timeout(self, e):
        # transaction
        self.log.error(
            'Timeout waiting for votes on tx %s: %s', self, e)
        self.abort()

    """ Actions """

    is_peerless = None

    def propose(self):
        """Flood peers with proposal for us to get stored."""

        # Ensure sequence is set
        if not self.sequence:
            # self.sequence = self._node.seq.allocate_pending()
            self.sequence = self._node.seq.pending_tx(self)

        self.is_peerless = not bool(self._node.peers)

        if not self.is_peerless:
            # With peers, propose and wait for votes
            self.broadcast('proposal', self.uuid, self.to_dict())
        else:
            # Without any peers, we simply commit.
            self.is_peerless = True
            self._debug('Committing tx %s as it is peerless.', self)
            self.commit()

    @xworkflows.transition()
    def abort(self):
        """Abort (proposed) tx."""
        self.log.warning('Aborting tx %s', self)
        self.broadcast('abort', channel=self.channel_tx)

    @xworkflows.transition()
    def commit(self):
        """Commit (proposed) tx."""

        # If we're in an proposal state, and not peerless, suicide
        if self.state.is_proposal and not self.is_peerless:
            raise xworkflows.InvalidTransitionError

        self.log.info('Committing tx %s', self)
        self.broadcast('commit', self.sequence, channel=self.channel_tx)
        self.store()

    @xworkflows.on_enter_state('commit')
    @xworkflows.on_enter_state('abort')
    def done(self, r):
        #if not (self.state.is_commit or self.state.is_abort):
        #    raise TransactionError("Cannot be done if not in commit or abort state")
        return _BaseTransaction.done(self)

    """ Handlers """

    #@xworkflows.transition()
    def receive_vote(self, peer, accept, meta):
        self.voting()

        """Sent by each peer that receives a proposal courtesy of self.propose()."""
        self.log.info(
            'Received vote on %s from %s: accept=%s meta=%s',
            self, peer, accept, meta)
        # self._debug('meta=%s', meta)

        self._votes[peer] = dict(
            accept=accept,
            sequence=meta['sequence'],
            cur_sequence=meta['cur_sequence'],
        )

        self.check_if_done_voting()

    #@xworkflows.after_transition('receive_vote')
    def check_if_done_voting(self, *args):
        # self._debug('args=%s', args)
        if len(self._votes) == len(self._node.peers):
            for k, v in self._votes.iteritems():
                if not v['accept']:
                    self.log.error(
                        'Aborting tx %s: peer %s did not accept.', self, k)
                    #raise PeerDidNotAccept(k)
                    if not self.state.is_abort:
                        self.abort()
                    #self.kill(exception=PeerDidNotAccept(k))
                    self.kill()
                    return

                if not v['sequence'] == self.sequence:
                    self.log.error(
                        'Aborting tx %s: peer %s sequence did not match (%s!=%s).',
                        self, k, self.sequence, v['sequence'])
                    #raise PeerSequenceDidNotMatch(k)
                    if not self.state.is_abort:
                        self.abort()
                    #self.kill(exception=PeerSequenceDidNotMatch(k))
                    self.kill()
                    return
            self.commit()


class ReceiveTransaction(_BaseTransaction, xworkflows.WorkflowEnabled, LogMixin):

    class State(xworkflows.Workflow):
        initial_state = 'proposal'
        states = (
            ('proposal',    'Proposal'),
            ('voting',      'Voting'),
            ('commit',      'Commit'),
            ('abort',       'Aborted'),
        )
        transitions = (
            ('vote', 'proposal', 'voting'),
            ('commit', 'voting', 'commit'),
            ('abort', ('proposal', 'voting', 'commit'), 'abort'),
        )

    state = State()

    def __init__(self, node, sender, **kwargs):
        _BaseTransaction.__init__(self, node, **kwargs)
        self.sender = sender

        self._post_init()

    def _main(self):
        return self.vote()

    def _on_timeout(self, e):
        self.log.error(
            'Timeout waiting for commit/abort on tx %s: %s', self, e)

    def __repr__(self):
        return "<%s uuid='%s'>" % (self.__class__.__name__, getattr(self, 'uuid', None))

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
        #accept = self.sequence > self._node.seq.current
        accept = self.sequence == self._node.seq.current + 1
        meta = self._vote_meta
        self._debug(
            'Sending vote for tx %s: accept=%s meta=%s', self, accept, meta)
        #self.unicast(self.sender, 'vote', self.uuid, accept, meta, channel=self.channel_tx)
        self.broadcast('vote', accept, meta, channel=self.channel_tx)

    @xworkflows.transition()
    def abort(self):
        self.log.warning('Aborting tx %s from %s (sequence=%s).',
                         self, self.sender, self.sequence)

    @xworkflows.transition()
    def commit(self):
        self.log.info('Committing tx %s (sequence=%s).',
                      self, self.sequence)
        self.store()

    @xworkflows.on_enter_state('commit')
    @xworkflows.on_enter_state('abort')
    def done(self, r):
        #if not (self.state.is_commit or self.state.is_abort):
        #    raise TransactionError("Cannot be done if not in commit or abort state")
        return _BaseTransaction.done(self)

    """ Handlers """

    def receive_abort(self, peer):
        """Sent by sender to indicate abortlation of this tx"""
        if peer != self.sender:
            return
        self.abort()

    def receive_commit(self, peer, sequence):
        """Sent by sender to indicate tx as good to commit."""
        if str(peer.uuid) != self.sender:
            self.log.error('Received commit from sender %s that does not match proposer %s', peer, self.sender)
            return
        self.commit()
