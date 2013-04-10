
import sh

from solarsan.cli.backend import AutomagicNode
from solarsan.target.cli import TargetsNode

from .base import PoolsNode


class Storage(AutomagicNode):
    def ui_child_pools(self):
        return PoolsNode()

    #def ui_child_resources(self):
    #    return ResourcesNode()

    def ui_child_targets(self):
        return TargetsNode()

    def ui_command_create_pool(self, name):
        '''
        create - Creates a storage Pool
        '''
        raise NotImplemented

    def ui_command_lsscsi(self):
        '''
        lsscsi - list SCSI devices (or hosts) and their attributes
        '''
        return sh.lsscsi()

    def ui_command_df(self):
        '''
        df - report file system disk space usage
        '''
        return sh.df('-h')

    def ui_command_lsblk(self):
        return sh.lsblk()
