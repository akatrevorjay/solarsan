
from pprint import pprint
from solarsan.pretty import pp

import solarsan.mongo

from solarsan import conf
from solarsan.core import logger

from solarsan.models import Config

from solarsan.cluster.models import Peer
from solarsan.configure.models import Nic, NicConfig

from solarsan.storage.pool import Pool
from solarsan.storage.volume import Volume

from solarsan.storage.drbd import DrbdPeer, DrbdResource

from solarsan.storage.device import Device, BaseDevice, Disk, Cache, Log, Spare, Mirror, Devices, Disks, Partition, Partitions

devs = Devices()
d = devs[0]
e = devs[15]
m = Mirror()
m.append(d)
m.append(e)
c = Cache(d)
#try:
#    m.append(c)
#except:
#    print "Good, '%s' cannot be added to '%s'" % (c, m)

pl = Peer.get_local()
stor = pl.storage

from solarsan.target.models import Target, iSCSITarget, SRPTarget
#Device, VolumeDevice, ResourceDevice
import solarsan.target.scst
from solarsan.target import scstadmin, scst
import solarsan.target.utils

from solarsan.ha.models import FloatingIP

import rpyc
