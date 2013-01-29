
from solarsan.pretty import pp
pprint = pp

import solarsan.mongo

from storage.pool import Pool
from storage.volume import Volume

from cluster.models import Peer

import rpc.client
#import rpc.rpc_storage

#c = rpc.client.ClientWithRetry()
#c.connect('tcp://localhost:1785')

c = rpc.client.Client(connect='tcp://localhost:1785')

#c._zerorpc_inspect_fill()


