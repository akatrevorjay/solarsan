
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

from target.models import Target, iSCSITarget, SRPTarget
import target.scst
import target.utils

import rpyc

san0 = Peer.objects.get(hostname='san0')
san1 = Peer.objects.get(hostname='san1')

