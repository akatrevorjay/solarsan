
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

#m = message.DictMessage()
#m['omg'] = True

#t = transaction.Transaction(n, payload=m)

#t.propose()

#n.loop.start()

class Debugger(object):
    def __getattribute__(self, key):
        if key.startswith('receive_'):
            return self._receive_debug
        else:
            return object.__getattribute__(self, key)

    def _receive_debug(self, *parts):
        logger.info('parts=%s', parts)

debugger = Debugger()

n.add_handler('dkv.transaction', debugger)


while True:
    gevent.sleep(0.1)

