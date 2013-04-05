
#from solarsan.core import logger
#from solarsan import conf
#from solarsan.exceptions import SolarSanError
#from solarsan.cluster.models import Peer
#from solarsan.storage.filesystem import Filesystem
#from solarsan.storage.utils import clean_name
#from solarsan.storage.pool import Pool
#from solarsan.storage.volume import Volume
#from solarsan.storage.snapshot import Snapshot
#from solarsan.storage.drbd import DrbdResource
#from solarsan.target.models import iSCSITarget
#from solarsan.exceptions import ZfsError
#from solarsan.ha.models import FloatingIP
#from solarsan.configure.models import Nic
#from solarsan.configure.config import write_network_interfaces_config
#from django.template.defaultfilters import capfirst
#import os
#import rpyc
#import sh
#import time
#import errno
#import rdma
#import libibtool
#import libibtool.discovery
#import libibtool.inquiry

from . import AutomagicNode
from solarsan.core.cli import SystemNode
from solarsan.storage.cli import Storage


"""
CLI Root
"""


class CliRoot(AutomagicNode):
    def __init__(self, *args, **kwargs):
        super(CliRoot, self).__init__(*args, **kwargs)

        """ Params examples
        self.define_config_group_param(
            'global', 'developer_mode', 'bool',
            'If true, enables developer mode.')

        print self.get_group_param('global', 'developer_mode')

        if conf.config.get('debug'):
            Developer(self)
        """

        self.define_config_group_param(
            'attribute', 'developer_mode', 'bool',
            'If true, enables developer mode.')

    """
    Nodes
    """

    def ui_child_system(self):
        return SystemNode()

    def ui_child_storage(self):
        return Storage()

    #def ui_child_cluster(self):
    #    return ClusterNode()

    #def ui_child_targets(self):
    #    return TargetsNode()

    #def ui_child_logs(self):
    #    return LogsNode()

    """
    Old Ye Stuffe
    """

    def summary(self):
        return ('Thar be dragons.', False)
        return ('Ready.', True)

    #def refresh(self):
    #    for child in self.children:
    #        child.refresh()

    #def refresh(self):
    #    self.refresh()
