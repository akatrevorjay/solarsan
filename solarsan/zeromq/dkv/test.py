
import gevent.monkey
gevent.monkey.patch_all()

from solarsan import conf
from solarsan.zeromq.dkv import message, server, client, serializer, encoder, node, transaction

n = node.Node()
n.bind('tcp://*:%s' % conf.ports.dkv_rtr, 'tcp://*:%s' % conf.ports.dkv_pub)
n.start()

m = message.DictMessage()
m['omg'] = True

t = transaction.Transaction(n, payload=m)

while True:
    gevent.sleep(1)
    t.propose()
