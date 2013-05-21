
import gevent.monkey
gevent.monkey.patch_all()

from solarsan import conf
#from solarsan.zeromq.dkv import message, server, client, serializer, encoder, node, transaction
from solarsan.zeromq.dkv import node, message, transaction, encoder


dkv_rtr = conf.ports.dkv_rtr
dkv_pub = conf.ports.dkv_pub
dkv_rtr2 = dkv_rtr + 1000
dkv_pub2 = dkv_pub + 1000

n = node.Node()
n.bind('tcp://*:%s' % dkv_rtr, 'tcp://*:%s' % dkv_pub)

n2 = node.Node()
n2.bind('tcp://*:%s' % dkv_rtr2, 'tcp://*:%s' % dkv_pub2)

n2.connect(n.uuid, 'tcp://127.0.0.1:%s' % dkv_rtr, 'tcp://127.0.0.1:%s' % dkv_pub)
n.connect(n2.uuid, 'tcp://127.0.0.1:%s' % dkv_rtr2, 'tcp://127.0.0.1:%s' % dkv_pub2)

n.start()
n2.start()

m = message.Message()
m['omg'] = True
t = transaction.Transaction(n, payload=m)
t.start()
del t

m2 = message.Message()
m2['omg'] = False
t2 = transaction.Transaction(n2, payload=m2)
t2.start()
del t2

while True:
    gevent.sleep(1)
    #t.propose()
