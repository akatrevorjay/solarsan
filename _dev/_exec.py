
from pprint import pprint
from solarsan.pretty import pp

import solarsan.mongo

from solarsan import conf, logging
logger = logging.getLogger(__name__)

from solarsan.models import Config

from solarsan.cluster.models import Peer
from solarsan.configure.models import Nic, NicConfig

from solarsan.storage.pool import Pool
from solarsan.storage.volume import Volume

from solarsan.storage.drbd import DrbdPeer, DrbdResource

from solarsan.storage.device import Device, BaseDevice, Disk, Cache, Log, Spare, Mirror, Devices, Disks, Partition, Partitions

devs = Devices()
pl = Peer.get_local()
stor = pl.storage

from solarsan.target.models import Target, iSCSITarget, SRPTarget
#Device, VolumeDevice, ResourceDevice
import solarsan.target.scst
from solarsan.target import scstadmin, scst
import solarsan.target.utils

from solarsan.ha.models import FloatingIP

import rpyc
