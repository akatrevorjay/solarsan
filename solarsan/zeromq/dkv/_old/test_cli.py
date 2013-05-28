
import gevent.monkey
gevent.monkey.patch_all()

from solarsan import logging, conf
logger = logging.getLogger(__name__)

from solarsan.cluster.models import Peer
pl = Peer.get_local()

from solarsan.zeromq.dkv import message, server, client, serializer, encoder, node, transaction

n = node.Node()
#n._bind('tcp://*:5000', 'tcp://*:5001')
#n.connect('tcp://localhost:5000')
n.connect_peer(pl)
n.start()

#tm = transaction.TransactionManager(n)

#m = message.DictMessage()
#m['omg'] = True

#t = transaction.Transaction(n, payload=m)

#t.propose()

#n.loop.start()



while True:
    gevent.sleep(0.1)
