
from solarsan.core import logger
from solarsan import conf
from solarsan.cluster.models import Peer
from solarsan.storage.filesystem import Filesystem
from solarsan.storage.utils import clean_name
from solarsan.storage.pool import Pool
from solarsan.storage.volume import Volume
from solarsan.storage.snapshot import Snapshot
from solarsan.exceptions import ZfsError
from setproctitle import setproctitle
import os
import rpyc
import sh
import time

#import rdma
#import libibtool
#import libibtool.discovery
#import libibtool.inquiry


class AutomagicNode(object):
    def get_ui_commands(self):
        ret = {}
        for attr in dir(self):
            if not attr.startswith('ui_command_'):
                continue
            ret[attr] = {}
        return ret

    def get_ui_children(self):
        ret = {}
        for attr in dir(self):
            if attr.startswith('ui_children_factory_'):
                if not attr.endswith('_list'):
                    continue

                factory = attr.rpartition('_list')[0]

                func = getattr(self, attr)
                for name in func():
                    ret[name] = dict(factory=factory)

            elif attr.startswith('ui_child_'):
                name = attr.partition('ui_child_')[2]
                ret[name] = {}
        return ret


class CLIService(rpyc.Service, AutomagicNode):
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

    def system(self):
        return SystemNode()

    def developer(self):
        return Developer()

    def benchmarks(self):
        return Benchmarks()

    def ui_child_storage(self):
        return Storage()

    def pools(self):
        return PoolsNode()

    def pool(self, pool):
        return PoolNode(pool)

    def volume(self, volume):
        return VolumeNode(volume)

    def ui_child_cluster(self):
        return ClusterNode()


class ClusterNode(AutomagicNode):
    def ui_command_info(self):
        return 'omg'

    def ui_child_peers(self):
        return PeersNode()


class PeersNode(AutomagicNode):
    def ui_children_factory_peer_list(self):
        return [p.hostname for p in Peer.objects.all()]

    def ui_children_factory_peer(self, name):
        return PeerNode(name)


class PeerNode(AutomagicNode):
    def __init__(self, hostname):
        self.obj = Peer.objects.get(hostname=hostname)


class PoolsNode(AutomagicNode):
    def ui_children_factory_pool_list(self):
        return [p.name for p in Pool.list()]

    def ui_children_factory_pool(self, name):
        return PoolNode(name)


class VolumeNode(object):
    def __init__(self, volume):
        self.obj = Volume(name=volume)


class SystemNode(object):
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
        return sh.shutdown('-h', 'now')

    def reboot(self):
        '''
        reboot - reboot system
        '''
        #status.tasks.reboot.delay()
        return sh.reboot()

    def check_services(self):
        return sh.egrep(sh.initctl('list'), 'solarsan|targetcli|mongo')


"""
Developer
"""


class Developer(object):
    def __init__(self):
        pass

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

    #def lsibdevices(self):
    #    return rdma.get_devices().keys()

    def ibstat(self):
        #return libibtool.inquiry.cmd_ibstat([], libibtool.tools.MyOptParse(
        return sh.ibstat()

    def ibstatus(self):
        return sh.ibstatus()

    def ibv_devinfo(self):
        return sh.ibv_devinfo()

    def ibping(self, host):
        return sh.ibping(host, _err_to_out=True)

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
            return line.rstrip("\n")

    def rdma(self, host=None):
        if host:
            logger.info("Running client to host='%s'", host)
            ret = sh.rdma_client(host, _err_to_out=True, _iter=True)
        else:
            logger.info("Running server on 0.0.0.0")
            ret = sh.rdma_server(_err_to_out=True, _iter=True)
        return ret


class Benchmarks(object):
    def __init__(self):
        pass

    def netperf(self, host=None):
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

    def cleanup(self, pool=None):
        if pool:
            self.test_pool = pool
        self._cleanup_test_filesystem()

    def bonniepp(self, atime='off', compress='on', pool=None):
        if pool:
            self.test_pool = pool
        fs = self._create_test_filesystem(atime=atime, compress=compress)

        bonniepp = sh.Command('bonnie++')
        ret = bonniepp('-u', 'nobody', '-d', str(fs.properties['mountpoint']),
                             _iter=True, _err_to_out=True)

        self._cleanup_test_filesystem()
        return ret

    def iozone(self, atime='off', compress='on', size='1M', pool=None):
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


class StorageNode(object):
    def __init__(self, obj):

        #obj_path = obj.path()
        #if hasattr(obj, 'children'):
        #    all_children = obj.children()
        #    if all_children:
        #        def find_children(depth):
        #            ret = []
        #            for child in all_children:
        #                child_path = child.path()
        #                child_depth = len(child_path)
        #                #if child_depth > max_child_depth:
        #                #    max_child_depth = child_depth
        #                if child_depth == depth:
        #                    ret.append(child)
        #            return ret
        #
        #        show_depth = len(obj_path) + 1
        #        children = find_children(show_depth)
        #
        #        for child in children:
        #            add_child_dataset(self, child)

        pass

    """
    Getters
    """

    def _get_pool(self):
        if self.obj.type == 'pool':
            return self.obj
        else:
            return self.obj.pool

    def _get_filesystem(self):
        if self.obj.type == 'pool':
            return self.obj.filesystem
        elif self.obj.type == 'filesystem':
            return self.obj

    """
    Child Creationism (Teach it to 'em young)
    """

    def create_snapshot(self, name):
        '''
        create - Creates a snapshot
        '''
        parent = self._get_filesystem()
        #pool = self._get_pool()
        cls = Snapshot
        name = clean_name(name)

        obj_name = os.path.join(parent.name, name)
        obj = cls(name=obj_name)
        if obj.exists():
            raise ZfsError("Object '%s' already exists", name)
        obj.create()
        return True

    def destroy(self, confirm=False):
        obj = self.obj
        if not obj.exists():
            raise ZfsError("Object '%s' does not exist", obj)
        #if not confirm:
        #    raise ZfsError("You must set confirm=True argument to confirm such an action of destruction")
        obj.destroy(confirm=confirm)
        return True

    def rename(self, new):
        path = self.obj.path(len=-1)
        path.append(clean_name(new))
        new = os.path.join(*path)
        obj = self.obj
        if not obj.exists():
            raise ZfsError("Object '%s' does not exist", obj)
        obj.rename(new)
        return True

    """
    Properties
    """

    POOL_PROPERTIES = []

    def ui_getgroup_property(self, property):
        '''
        This is the backend method for getting propertys.
        @param property: The property to get the value of.
        @type property: str
        @return: The property's value
        @rtype: arbitrary
        '''
        if property in self.POOL_PROPERTIES:
            obj = self._get_pool()
        else:
            obj = self._get_filesystem()
        return str(obj.properties[property])

    def ui_setgroup_property(self, property, value):
        '''
        This is the backend method for setting propertys.
        @param property: The property to set the value of.
        @type property: str
        @param value: The property's value
        @type value: arbitrary
        '''
        if property in self.POOL_PROPERTIES:
            obj = self._get_pool()
        else:
            obj = self._get_filesystem()
        obj.properties[property] = value

    POOL_STATISTICS = ['dedupratio']

    def ui_getgroup_statistic(self, statistic):
        '''
        This is the backend method for getting statistics.
        @param statistic: The statistic to get the value of.
        @type statistic: str
        @return: The statistic's value
        @rtype: arbitrary
        '''
        if statistic in self.POOL_STATISTICS:
            obj = self._get_pool()
        else:
            obj = self._get_filesystem()

        return str(obj.properties[statistic])

    def ui_setgroup_statistic(self, statistic, value):
        '''
        This is the backend method for setting statistics.
        @param statistic: The statistic to set the value of.
        @type statistic: str
        @param value: The statistic's value
        @type value: arbitrary
        '''
        #self.obj.properties[statistic] = value
        return None


#def add_child_dataset(self, child):
#    if child.type == 'filesystem':
#        DatasetNode(self, child)
#    elif child.type == 'volume':
#        DatasetNode(self, child)
#    elif child.type == 'snapshot':
#        DatasetNode(self, child)


#class StorageNodeChildType(ConfigNode):
#    def __init__(self, parent, child_type):
#        self.child_type = child_type
#        super(StorageNodeChildType, self).__init__('%ss' % child_type, parent)


class DatasetNode(StorageNode):
    def __init__(self, parent, dataset):
        super(DatasetNode, self).__init__(parent, dataset)

    #def ui_type_blah(self):
    #    pass

    def summary(self):
        # TODO Check disk usage percentage, generic self.obj.errors/warnings
        # interface perhaps?
        return (self.obj.type, True)


class PoolNode(StorageNode):
    def __init__(self, pool):
        self.obj = Pool(name=pool)
        super(PoolNode, self).__init__(pool)

    def summary(self):
        return (self.obj.health, self.obj.is_healthy())

    def usage(self):
        obj = self.obj
        alloc = str(obj.properties['alloc'])
        free = str(obj.properties['free'])
        total = str(obj.properties['size'])
        ret = {'alloc': alloc,
               'free': free,
               'total': total,
               }
        return ret

    """
    Children
    """

    #def create_filesystem(self, name):
    #    '''
    #    create - Creates a Filesystem
    #    '''
    #    parent = self._get_filesystem()
    #    pool = self._get_pool()
    #    cls = m.Filesystem
    #    name = clean_name(name)
    #
    #    obj_name = os.path.join(parent.name, name)
    #    obj = cls(name=obj_name)
    #    if obj.exists():
    #        raise ZfsError("Object '%s' already exists", name)
    #    obj.create()

    def create_volume(self, name, size):
        '''
        create - Creates a volume
        '''
        parent = self._get_filesystem()
        #pool = self._get_pool()
        cls = Volume
        name = clean_name(name)

        obj_name = os.path.join(parent.name, name)
        obj = cls(name=obj_name)
        if obj.exists():
            raise ZfsError("Object '%s' already exists", name)
        obj.create(size)
        return True


    def volumes(self):
        return self.obj.volumes(ret_obj=False, ret=dict)

    """
    Devices
    """

    def lsdevices(self):
        return self.obj.devices()

    lsd = lsdevices

    #def add_device(self, path, type='disk'):
    #    logging.error("TODO")

    #def replace_device(self, old, new):
    #    logging.error("TODO")

    """
    Status
    """

    def iostat(self, capture_length=2):
        return self.obj.iostat(capture_length=capture_length)

    def status(self):
        return self.obj.status()

    def health(self):
        return str(self.obj.properties['health'])

    def clear(self):
        return self.obj.clear()

    """
    Import/Export
    """

    def import_(self):
        return self.obj.import_()

    def export(self):
        return self.obj.export()

    """
    Cluster
    """

    ## TODO Attribute
    #def is_clustered(self):
    #    return self.obj.is_clustered


class Storage(object):
    def __init__(self):
        pass

    def create_pool(self, name):
        '''
        create - Creates a storage Pool
        '''
        raise NotImplemented

    def lsscsi(self):
        '''
        lsscsi - list SCSI devices (or hosts) and their attributes
        '''
        return sh.lsscsi()

    def df(self):
        '''
        df - report file system disk space usage
        '''
        return sh.df('-h')

    def lsblk(self):
        return sh.lsblk()


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
    from setproctitle import setproctitle
    title = 'SolarSan CLI'
    setproctitle('[%s]' % title)

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
