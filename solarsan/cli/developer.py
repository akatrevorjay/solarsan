
from solarsan.core import logger
from solarsan import conf
from configshell import ConfigNode
from .benchmarks import Benchmarks
import os
import sh


"""
Developer
"""


class Developer(ConfigNode):
    def __init__(self, parent):
        super(Developer, self).__init__('developer', parent)
        Benchmarks(self)

    def ui_command_shell(self):
        os.system("bash")

    def ui_command_pyshell(self):
        os.system("/opt/solarsanweb/manage shell_plus")

    def ui_command_ipyshell(self):
        os.system("/opt/solarsanweb/manage shell_plus --ipython")

    def ui_command_ipynotebook(self):
        os.system("/opt/solarsanweb/manage ipython notebook --ext=django_notebook")

    def ui_command_mongo(self):
        os.system("mongo")

    def ui_command_stop_services(self):
        os.system("stop solarsan")

    def ui_command_start_services(self):
        os.system("start solarsan")

    def ui_command_targetcli(self):
        os.system("targetcli")

    #def ui_command_export_clustered_pool_vdevs(self):
    #    cluster.tasks.export_clustered_pool_vdevs.apply()

    def ui_command_top(self):
        os.system("top")

    def ui_command_ps(self):
        os.system("ps aux")

    def ui_command_pstree(self):
        os.system("pstree -ahuU")

    def ui_command_iostat(self):
        os.system("iostat -m 5 2")

    def ui_command_zpool_iostat(self):
        os.system("zpool iostat -v 5 2")

    def ui_command_ibstat(self):
        os.system("ibstat")

    def ui_command_ibstatus(self):
        os.system("ibstatus")

    def ui_command_ibv_devinfo(self):
        os.system("ibv_devinfo")

    def ui_command_ibping(self, host):
        print sh.ibping(host, _err_to_out=True)

    def ui_command_ibrouters(self):
        os.system("ibrouters")

    def ui_command_ibswitches(self):
        os.system("ibswitches")

    def ui_command_ibdiscover(self):
        os.system("ibdiscover")

    def ui_command_ibnodes(self):
        os.system("ibnodes")

    def ui_command_ibtool(self, *args):
        for line in sh.ibtool(*args, _iter=True, _err_to_out=True):
            print line.rstrip("\n")

    def ui_command_rdma(self, host=None):
        if host:
            logger.info("Running client to host='%s'", host)
            ret = sh.rdma_client(host, _err_to_out=True, _iter=True)
        else:
            logger.info("Running server on 0.0.0.0")
            ret = sh.rdma_server(_err_to_out=True, _iter=True)
        for line in ret:
            print line.rstrip("\n")

    #def ui_command_ibstat(self):
    #    os.system("ibstat")

    #def ui_command_ibstat(self):
    #    os.system("ibstat")

    def ui_command_ipdb(self):
        import ipdb
        ipdb.set_trace()

    #def ui_command_ipdb_post_mortem(self):
    #    import ipdb
    #    ipdb.pm()


