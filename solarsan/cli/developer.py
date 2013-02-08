
#from solarsan.core import logger
#from solarsan import conf
from .benchmarks import Benchmarks
from .base import BaseServiceConfigNode


"""
Developer
"""


class Developer(BaseServiceConfigNode):
    def __init__(self, parent):
        super(Developer, self).__init__(None, parent)
        Benchmarks(self)

    #def ui_command_shell(self):
    #    self()

    #def ui_command_pyshell(self):
    #    self()

    #def ui_command_ipyshell(self):
    #    self()

    #def ui_command_ipynotebook(self):
    #    self()

    #def ui_command_mongo(self):
    #    self()

    def ui_command_stop_services(self):
        '''
        stop_services - Stops SolarSan services
        '''
        self()

    def ui_command_start_services(self):
        '''
        start_services - Starts SolarSan services
        '''
        self()

    #def ui_command_targetcli(self):
    #    self()

    #def ui_command_export_clustered_pool_vdevs(self):
    #    cluster.tasks.export_clustered_pool_vdevs.apply()

    #def ui_command_top(self):
    #    self()

    def ui_command_ps(self):
        self()

    def ui_command_pstree(self):
        self()

    def ui_command_iostat(self):
        self()

    def ui_command_zpool_iostat(self):
        self()

    def ui_command_ibstat(self):
        self()

    def ui_command_ibstatus(self):
        self()

    def ui_command_ibv_devinfo(self):
        self()

    def ui_command_ibping(self, host):
        self()

    def ui_command_ibrouters(self):
        self()

    def ui_command_ibswitches(self):
        self()

    def ui_command_ibdiscover(self):
        self()

    def ui_command_ibnodes(self):
        self()

    def ui_command_ibtool(self, *args):
        self(*args)

    def ui_command_rdma(self, host=None):
        self(host=host)
