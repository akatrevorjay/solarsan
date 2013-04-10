
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.exceptions import ZfsError
from solarsan.cli.backend import AutomagicNode
import os
import time
import sh

from solarsan.storage.filesystem import Filesystem


"""
Developer
"""


class Developer(AutomagicNode):
    def ui_child_benchmarks(self):
        return Benchmarks()

    #def ui_command_shell(self):
    #    return sh.bash")

    #def ui_command_pyshell(self):
    #    return sh."/opt/solarsanweb/manage shell_plus")

    #def ui_command_ipyshell(self):
    #    return sh."/opt/solarsanweb/manage shell_plus --ipython")

    #def ui_command_ipynotebook(self):
    #    return sh."/opt/solarsanweb/manage ipython notebook --ext=django_notebook")

    #def ui_command_mongo(self):
    #    return sh."mongo")

    def ui_command_stop_services(self):
        return sh.stop("solarsan")

    def ui_command_start_services(self):
        return sh.start("solarsan")

    #def ui_command_targetcli(self):
    #    return sh.targetcli("targetcli")

    #def ui_command_export_clustered_pool_vdevs(self):
    #    cluster.tasks.export_clustered_pool_vdevs.apply()

    #def ui_command_top(self):
    #    return sh.top()

    def ui_command_ps(self):
        return sh.ps("aux")

    def ui_command_pstree(self):
        return sh.pstree("-ahuU")

    def ui_command_iostat(self):
        return sh.iostat('-m', '5', '2')

    def ui_command_zpool_iostat(self):
        return sh.zpool('iostat', '-v', '5', '2')

    #def ui_command_lsibdevices(self):
    #    return rdma.get_devices().keys()

    def ui_command_ibstat(self):
        #return libibtool.inquiry.cmd_ibstat([], libibtool.tools.MyOptParse(
        return sh.ibstat()

    def ui_command_ibstatus(self):
        return sh.ibstatus()

    def ui_command_ibv_devinfo(self):
        return sh.ibv_devinfo()

    def ui_command_ibping(self, host):
        return sh.ibping(host, _err_to_out=True)

    def ui_command_ibrouters(self):
        return sh.ibrouters()

    def ui_command_ibswitches(self):
        return sh.ibswitches()

    def ui_command_ibdiscover(self):
        return sh.ibdiscover()

    def ui_command_ibnodes(self):
        return sh.ibnodes()

    def ui_command_ibtool(self, *args):
        for line in sh.ibtool(*args, _iter=True, _err_to_out=True):
            return line.rstrip("\n")

    def ui_command_rdma(self, host=None):
        if host:
            logger.info("Running client to host='%s'", host)
            ret = sh.rdma_client(host, _err_to_out=True, _iter=True)
        else:
            logger.info("Running server on 0.0.0.0")
            ret = sh.rdma_server(_err_to_out=True, _iter=True)
        return ret


class Benchmarks(AutomagicNode):
    def ui_command_netperf(self, host=None):
        args = []
        if host:
            logger.info("Running client to host='%s'", host)
            args.extend(['-h', host])
        else:
            logger.info("Running server on 0.0.0.0")
        return sh.NPtcp(*args, _iter=True, _err_to_out=True)

    test_pool = 'dpool'
    test_filesystem_name = '%(pool)s/omfg_test_benchmark'

    def _get_test_filesystem(self):
        pool = self.test_pool
        name = self.test_filesystem_name % {'pool': pool}
        fs = Filesystem(name=name)
        return fs

    def _create_test_filesystem(self, atime='off', compress='on'):
        fs = self._get_test_filesystem()
        if fs.exists():
            logger.info("Destroying existing test filesystem '%s'", fs)
            fs.destroy(confirm=True)

        logger.info("Creating test filesystem '%s'", fs)
        fs.create()

        logger.info("Setting atime='%s' compress='%s'", atime, compress)
        fs.properties['atime'] = atime
        fs.properties['compress'] = compress

        logger.info("Changing ownership")
        sh.chown('nobody:nogroup', str(fs.properties['mountpoint']))

        return fs

    def _cleanup_test_filesystem(self, pool=None):
        if pool:
            self.test_pool = pool
        fs = self._get_test_filesystem()
        if not fs.exists():
            raise ZfsError("Could not cleanup filesystem '%s' as it does not exist?", fs)
        logger.info("Destroying test filesystem '%s'", fs)
        fs.destroy(confirm=True)

    def ui_command_cleanup(self, pool=None):
        if pool:
            self.test_pool = pool
        self._cleanup_test_filesystem()

    def ui_command_bonniepp(self, atime='off', compress='on', pool=None):
        if pool:
            self.test_pool = pool
        fs = self._create_test_filesystem(atime=atime, compress=compress)

        bonniepp = sh.Command('bonnie++')
        ret = bonniepp('-u', 'nobody', '-d', str(fs.properties['mountpoint']),
                             _iter=True, _err_to_out=True)

        self._cleanup_test_filesystem()
        return ret

    def ui_command_iozone(self, atime='off', compress='on', size='1M', pool=None):
        if pool:
            self.test_pool = pool
        fs = self._create_test_filesystem(atime=atime, compress=compress)

        cwd = os.curdir
        os.chdir(str(fs.properties['mountpoint']))

        try:
            with sh.sudo('-u', 'nobody', _with=True):
                ret = sh.iozone('-a', '-g', size, _iter=True, _err_to_out=True)
        finally:
            os.chdir(cwd)

        time.sleep(1)
        self._cleanup_test_filesystem()
        return ret
