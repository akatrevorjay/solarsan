

class Transaction(object):
    """Transaction class that handles any updates on their path toward good,
    or evil."""
    ts = None
    payload = None
    uuid = None

    _serialize_attrs = ['ts', 'payload', 'uuid']

    _min_sequence = None
    _agent = None
    _replies = None
    _votes = None

    def __init__(self, agent, **kwargs):
        self._agent = agent
        self._replies = {}
        self._votes = {}

        if kwargs:
            self.update(kwargs)

        self.bar = self._bar

    @classmethod
    def from_dict(cls, agent, datadict):
        return cls(agent, **datadict)

    @classmethod
    def bar(cls, baz):
        print "bar, baz:", baz

    def _bar(self, baz):
        print "_bar, baz:", baz

    def _from_dict(self, datadict):
        pass

    def update(self, datadict):
        if not datadict:
            return
        for k in self._serialize_attrs:
            setattr(self, k, datadict.get(k))

    def to_dict(self):
        ret = {}
        for k in self._serialize_attrs:
            ret[k] = getattr(self, k, None)
        return ret

    def publish(self, sock):
        #sock = self._agent.publisher
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
