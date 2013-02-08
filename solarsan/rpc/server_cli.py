
from solarsan.core import logger
from solarsan import conf
from solarsan.storage.filesystem import Filesystem
from solarsan.storage.volume import Volume
from solarsan.storage.pool import Pool
from solarsan.exceptions import FormattedException, ZfsError
import os
import rpyc
import sh
import time


class CLIService(rpyc.Service):
    def on_connect(self):
        logger.debug('Client connected.')

    def on_disconnect(self):
        logger.debug('Client disconnected.')

    def ping(self):
        return True

    # Override the stupid prepending of expose_prefix to attrs, why is the
    # config not honored??
    def _rpyc_getattr(self, name):
        return getattr(self, name)

    #def _rpyc_delattr(self, name):
    #    pass

    #def _rpyc_setattr(self, name, value):
    #    pass

    """
    Nodes
    """

    def get_nodes(self):
        pass

    def system(self):
        return System()


class System(object):
    def __init__(self):
        pass

    def hostname(self):
        '''
        Displays the system hostname
        '''
        return sh.hostname('-f')

    def uname(self):
        '''
        Displays the system uname information.
        '''
        return sh.uname('-a')

    def lsmod(self):
        '''
        lsmod - program to show the status of modules in the Linux Kernel
        '''
        return sh.lsmod()

    def lspci(self):
        '''
        lspci - list all PCI devices
        '''
        return sh.lspci()

    def lsusb(self):
        '''
        lsusb - list USB devices
        '''
        return sh.lsusb()

    def lscpu(self):
        '''
        lscpu - CPU architecture information helper
        '''
        return sh.lscpu()

    def lshw(self):
        '''
        lshw - List all hardware known by HAL
        '''
        return sh.lshw()

    def uptime(self):
        '''
        uptime - Tell how long the system has been running.
        '''
        return sh.uptime()

    def shutdown(self):
        '''
        shutdown - Shutdown system
        '''
        #status.tasks.shutdown.delay()
        raise NotImplemented

    def reboot(self):
        '''
        reboot - reboot system
        '''
        #status.tasks.reboot.delay()
        raise NotImplemented

    def check_services(self):
        return sh.egrep(sh.initctl('list'), 'solarsan|targetcli|mongo')


"""
Developer
"""


class Developer(object):
    def __init__(self, parent):
        super(Developer, self).__init__('developer', parent)
        Benchmarks(self)

    #def shell(self):
    #    return sh.bash")

    #def pyshell(self):
    #    return sh."/opt/solarsanweb/manage shell_plus")

    #def ipyshell(self):
    #    return sh."/opt/solarsanweb/manage shell_plus --ipython")

    #def ipynotebook(self):
    #    return sh."/opt/solarsanweb/manage ipython notebook --ext=django_notebook")

    #def mongo(self):
    #    return sh."mongo")

    def stop_services(self):
        return sh.stop("solarsan")

    def start_services(self):
        return sh.start("solarsan")

    #def targetcli(self):
    #    return sh.targetcli("targetcli")

    #def export_clustered_pool_vdevs(self):
    #    cluster.tasks.export_clustered_pool_vdevs.apply()

    #def top(self):
    #    return sh.top()

    def ps(self):
        return sh.ps("aux")

    def pstree(self):
        return sh.pstree("-ahuU")

    def iostat(self):
        return sh.iostat('-m', '5', '2')

    def zpool_iostat(self):
        return sh.zpool('iostat', '-v', '5', '2')

    def ibstat(self):
        return sh.ibstat()

    def ibstatus(self):
        return sh.ibstatus()

    def ibv_devinfo(self):
        return sh.ibv_devinfo()

    def ibping(self, host):
        print sh.ibping(host, _err_to_out=True)

    def ibrouters(self):
        return sh.ibrouters()

    def ibswitches(self):
        return sh.ibswitches()

    def ibdiscover(self):
        return sh.ibdiscover()

    def ibnodes(self):
        return sh.ibnodes()

    def ibtool(self, *args):
        for line in sh.ibtool(*args, _iter=True, _err_to_out=True):
            print line.rstrip("\n")

    def rdma(self, host=None):
        if host:
            logger.info("Running client to host='%s'", host)
            ret = sh.rdma_client(host, _err_to_out=True, _iter=True)
        else:
            logger.info("Running server on 0.0.0.0")
            ret = sh.rdma_server(_err_to_out=True, _iter=True)
        for line in ret:
            print line.rstrip("\n")

    #def ibstat(self):
    #    return sh."ibstat")

    #def ibstat(self):
    #    return sh."ibstat")

    #def ipdb(self):
    #    import ipdb
    #    ipdb.set_trace()

    #def ipdb_post_mortem(self):
    #    import ipdb
    #    ipdb.pm()


class Benchmarks(object):
    def __init__(self, parent):
        super(Benchmarks, self).__init__('benchmarks', parent)

    def netperf(self, host=None):
        args = []
        if host:
            logger.info("Running client to host='%s'", host)
            args.extend(['-h', host])
        else:
            logger.info("Running server on 0.0.0.0")
        for line in sh.NPtcp(*args, _iter=True, _err_to_out=True):
            print line.rstrip("\n")

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

    def cleanup(self, pool=None):
        if pool:
            self.test_pool = pool
        self._cleanup_test_filesystem()

    def bonniepp(self, atime='off', compress='on', pool=None):
        if pool:
            self.test_pool = pool
        fs = self._create_test_filesystem(atime=atime, compress=compress)

        bonniepp = sh.Command('bonnie++')
        for line in bonniepp('-u', 'nobody', '-d', str(fs.properties['mountpoint']),
                             _iter=True, _err_to_out=True):
            print line.rstrip("\n")

        self._cleanup_test_filesystem()

    def iozone(self, atime='off', compress='on', size='1M', pool=None):
        if pool:
            self.test_pool = pool
        fs = self._create_test_filesystem(atime=atime, compress=compress)

        cwd = os.curdir
        os.chdir(str(fs.properties['mountpoint']))

        try:
            with sh.sudo('-u', 'nobody', _with=True):
                for line in sh.iozone('-a', '-g', size, _iter=True, _err_to_out=True):
                    print line.rstrip("\n")
        finally:
            os.chdir(cwd)

        time.sleep(1)
        self._cleanup_test_filesystem()


class CliRoot(object):
    def __init__(self, shell, sections):
        pass

    def summary(self):
        #return ('Thar be dragons.', False)
        return ('Ready.', True)

    def new_node(self, new_node):
        logger.info("New node: %s", new_node)
        return None

    #def refresh(self):
    #    for child in self.children:
    #        child.refresh()

    #def refresh(self):
    #    self.refresh()

    #def ui_getgroup_global(self, key):
    #    '''
    #    This is the backend method for getting keys.
    #    @key key: The key to get the value of.
    #    @type key: str
    #    @return: The key's value
    #    @rtype: arbitrary
    #    '''
    #    logger.info("attr=%s", key)
    #    #return self.rtsnode.get_global(key)

    #def ui_setgroup_global(self, key, value):
    #    '''
    #    This is the backend method for setting keys.
    #    @key key: The key to set the value of.
    #    @type key: str
    #    @key value: The key's value
    #    @type value: arbitrary
    #    '''
    #    logger.info("attr=%s val=%s", key, value)
    #    #self.assert_root()
    #    #self.rtsnode.set_global(key, value)

    #def ui_getgroup_param(self, param):
    #    '''
    #    This is the backend method for getting params.
    #    @param param: The param to get the value of.
    #    @type param: str
    #    @return: The param's value
    #    @rtype: arbitrary
    #    '''
    #    logger.info("attr=%s", param)
    #    #return self.rtsnode.get_param(param)

    #def ui_setgroup_param(self, param, value):
    #    '''
    #    This is the backend method for setting params.
    #    @param param: The param to set the value of.
    #    @type param: str
    #    @param value: The param's value
    #    @type value: arbitrary
    #    '''
    #    logger.info("attr=%s val=%s", param, value)
    #    #self.assert_root()
    #    #self.rtsnode.set_param(param, value)

    #def ui_getgroup_attribute(self, attribute):
    #    '''
    #    This is the backend method for getting attributes.
    #    @param attribute: The attribute to get the value of.
    #    @type attribute: str
    #    @return: The attribute's value
    #    @rtype: arbitrary
    #    '''
    #    logger.info("attr=%s", attribute)
    #    #return self.rtsnode.get_attribute(attribute)

    #def ui_setgroup_attribute(self, attribute, value):
    #    '''
    #    This is the backend method for setting attributes.
    #    @param attribute: The attribute to set the value of.
    #    @type attribute: str
    #    @param value: The attribute's value
    #    @type value: arbitrary
    #    '''
    #    logger.info("attr=%s val=%s", attribute, value)
    #    #self.assert_root()
    #    #self.rtsnode.set_attribute(attribute, value)

    #def ui_getgroup_parameter(self, parameter):
    #    '''
    #    This is the backend method for getting parameters.
    #    @param parameter: The parameter to get the value of.
    #    @type parameter: str
    #    @return: The parameter's value
    #    @rtype: arbitrary
    #    '''
    #    logger.info("parameter=%s", parameter)
    #    #return self.rtsnode.get_parameter(parameter)

    #def ui_setgroup_parameter(self, parameter, value):
    #    '''
    #    This is the backend method for setting parameters.
    #    @param parameter: The parameter to set the value of.
    #    @type parameter: str
    #    @param value: The parameter's value
    #    @type value: arbitrary
    #    '''
    #    logger.info("parameter=%s val=%s", parameter, value)
    #    #self.assert_root()
    #    #self.rtsnode.set_parameter(parameter, value)


def main():
    from cluster.models import Peer
    from rpyc.utils.server import ThreadedServer

    local = Peer.get_local()
    cluster_iface_bcast = local.cluster_nic.broadcast
    # Allow all public attrs, because exposed_ is stupid and should be a
    # fucking decorator.
    t = ThreadedServer(CLIService, port=18863,
                       registrar=rpyc.utils.registry.UDPRegistryClient(ip=cluster_iface_bcast, logger=logger),
                       auto_register=True, logger=logger, protocol_config=conf.rpyc_conn_config)
    t.start()


if __name__ == '__main__':
    main()
