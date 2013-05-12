

# Start CloneServer in INITIAL state

# CloneServer creates local Beacon instance

# Beacon sends out discovery broadcasts, looking for peers

# Upon peer broadcast received, beacon connects to peer on beacon port via
# DEALER

# Upon peer connection, peer sends out GREET:

"""A listing of it's services and small informational about itself"""
GREET = dict(
    hostname='san0',
    uuid='blah-blah-blah',
    cluster_iface='eth1',
    services=dict(
        storage=5000,
        dkv=5001,
        cli=5002,
    ),
)

# When GREET is received:

# - Create ClonePeer instance from GREET
#   - If no Peer object exists for this Peer's uuid, create it.
#   - Connect DKV collector
#   - Connect DKV subscriber

# - Add peer to CloneServer peers (as ClonePeer)


# kvmsg can contain the methods for signatures and verification.


# Can't kvmsg's just be dicts? It's kind of stupid the way it is now,
# IMO. Plus then it would serialize even with JSON, ya dig?


# What if we did keep an array of versions of the kvmap with the changes in
# each level?
#
# It would allow for synchronous replication and rollbacks. Actually, so does
# just making sure a transaction was accepted by all before "commit"ing it.
#
# What if two requests come in on different nodes during the same sequence?
# It sounds to be like the sequence id should not be set until after the
# transaction has been published (as pending) to all nodes, yet not committed.
#
# This means we can take in more than one at once and even from multiple nodes
# as it will get a sequence number after publishing the transaction to pending.
#
# When a node receives a pending transaction, it can return a sequence vote.
# The publisher can just take the highest value, and then in the following
# commit request for that transaction, specify what the winning sequence was.
# If we run into another collision, we just wait a random small amount of time,
# allowing some network messages to travel to finish up the fanouts (or deaths
# of nodes being found, etc), then retry the commit.
#
# Pending transactions should be stored with a TTL (or a timeout set). Upon TTL
# expiry, the node will tell the publisher that it has expired it's
# transaction. The sender can at that point give up on that transaction,
# complain loudly and move on.


from datetime import datetime, timedelta


class Transaction:
    class STATES:
        INITIAL = 0
        PENDING = 2
        COMMITING = 3
        COMMITTED = 4
        ERROR = -1

    state = None

    def __init__(self, payload, parent=None):
        assert parent
        self.parent = parent
        self.ts = datetime.now()
        self.payload = payload

        self.replies = {}
        self.state = self.STATES.INITIAL

    def serialize(self):
        return self.__dict__.copy()

    def send(self):
        self.parent.publisher.send(self.serialize())
        return True

    def on_collect_pending(self, sequence):
        self.state = self.STATES.PENDING
        sequence = int(sequence)
        reply = self.replies.get(sequence, 0)
        self.replies[sequence] = reply + 1

    def commit(self, sequence):
        sequence = int(sequence)
        self.state = self.STATES.COMMITING
        self.state = self.STATES.COMMITTED
        return True

# CloneServer.pending_transactions = {trans_uuid: Transaction()}

# cli: Trying to make an update; create dkv transaction object
#   cli-pub >> srv-rtr: Transaction(payload)
# srv: Save the dkv update as pending
#   self.pending_transactions.append(dkv_transaction_obj)

# srv: Publish this transaction to all connected peers
#   for peer in self.peers:
#       try:
#           dkv_transaction_obj.publish_pending_transaction(peer)
#       except PublishError:
#           # rollback transaction


#


##
## p2p way
##

# Client keeps connections open with all beaconed peers (already does this to a
# point, just needs more sockets connected in the callack)
#
# Differences:
# - Instead of looping through each server in the list until one
# works (ie finishes a send state):
#   - Open socket to collector, send a PING and if we don't get a PONG in
#   timeout/s then disconnect; Do not retry (ie accept another beacon) until
#   after server_timeout_retry/s.
#   -

# When a client has to sent out an update,


