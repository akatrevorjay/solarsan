
from solarsan.pretty import pp
pprint = pp

import solarsan.mongo

from storage.pool import Pool
from storage.volume import Volume
from storage.drbd import DrbdPeer, DrbdResource
from cluster.models import Peer
from configure.models import Nic, NicConfig
from solarsan.models import Config
from solarsan import conf
from solarsan.core import logger
from solarsan.target.models import Target, iSCSITarget, SRPTarget


#import rpc.client
#import rpc.rpc_storage

#c = rpc.client.ClientWithRetry()
#c.connect('tcp://localhost:1785')

#c = rpc.client.Client(connect='tcp://localhost:1785')

#c._zerorpc_inspect_fill()


import rpyc
#c_storage_any = rpyc.connect_by_service('storage')

san0 = Peer.objects.get(hostname='san0')
san1 = Peer.objects.get(hostname='san1')

