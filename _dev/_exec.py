
from pprint import pprint
from solarsan.pretty import pp

import solarsan.mongo

from solarsan import conf
from solarsan.core import logger

from solarsan.models import Config

from cluster.models import Peer
from configure.models import Nic, NicConfig

from storage.pool import Pool
from storage.volume import Volume

from storage.drbd import DrbdPeer, DrbdResource

from storage.device import Device, BaseDevice, Disk, Cache, Log, Spare, Mirror, Devices, Disks, Partitions

devs = Devices()
d = devs[0]
e = devs[15]
m = Mirror()
m.append(d)
m.append(e)
c = Cache(d)
try:
    m.append(c)
except:
    print "Good, '%s' cannot be added to '%s'" % (c, m)

from target.models import Target, iSCSITarget, SRPTarget
#Device, VolumeDevice, ResourceDevice
import target.scst
from target import scstadmin, scst
import target.utils

from ha.models import ActivePassiveIP

import rpyc

san0 = Peer.objects.get(hostname='san0')
san1 = Peer.objects.get(hostname='san1')


