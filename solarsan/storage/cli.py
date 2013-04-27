
import sh

from solarsan.cli.backend import AutomagicNode
from solarsan.target.cli import TargetsNode


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


from solarsan.exceptions import ZfsError
from solarsan.storage.utils import clean_name
from django.template.defaultfilters import capfirst
import os

from solarsan.cli.backend import AutomagicNode

from .drbd import DrbdResource
from .pool import Pool
#from .filesystem import Filesystem
from .volume import Volume
from .snapshot import Snapshot


class DatasetPropsMixIn:
    def __init__(self):
        self.define_config_group_param('dataset', 'compression', 'string', 'Enable compression')
        self.define_config_group_param('dataset', 'dedup', 'string', 'Enable dedupe')
        self.define_config_group_param('dataset', 'atime', 'string', 'Keep file access times up to date')
        self.define_config_group_param('dataset', 'quota', 'string', 'Quota for dataset')

        self.define_config_group_param('dataset', 'compressratio', 'string', 'Compresstion ratio', writable=False)
        self.define_config_group_param('dataset', 'used', 'string', 'Used space', writable=False)
        self.define_config_group_param('dataset', 'usedbysnapshots', 'string', 'Used space by snapshots', writable=False)
        self.define_config_group_param('dataset', 'usedbydataset', 'string', 'Used space by dataset', writable=False)
        self.define_config_group_param('dataset', 'usedbychildren', 'string', 'Used space by children', writable=False)
        self.define_config_group_param('dataset', 'usedbyrefreservation', 'string', 'Used space by referenced reservation', writable=False)
        self.define_config_group_param('dataset', 'referenced', 'string', 'Referenced space', writable=False)
        self.define_config_group_param('dataset', 'available', 'string', 'Available space', writable=False)

        #self.define_config_group_param('dataset', 'creation', 'string', 'Creation date', writable=False)
        self.define_config_group_param('dataset', 'mounted', 'bool', 'Currently mounted', writable=False)

    def ui_getgroup_dataset(self, key):
        '''
        This is the backend method for getting keys.
        @param key: The key to get the value of.
        @type key: str
        @return: The key's value
        @rtype: arbitrary
        '''
        obj = self._get_dataset()
        return str(obj.properties.get(key))

    def ui_setgroup_dataset(self, key, value):
        '''
        This is the backend method for setting keys.
        @param key: The key to set the value of.
        @type key: str
        @param value: The key's value
        @type value: arbitrary
        '''
        obj = self._get_dataset()
        obj.properties[key] = value


class PoolPropsMixIn:
    def __init__(self):
        self.define_config_group_param('pool', 'comment', 'string', 'Comment')
        self.define_config_group_param('pool', 'dedupditto', 'string', 'Number of copies of each deduplicated block to save')
        self.define_config_group_param('pool', 'autoexpand', 'string', 'Automatically expand pool if drives increase in size')
        self.define_config_group_param('pool', 'autoreplace', 'string', 'Automatically replace failed drives with any specified hot spare(s)')

        self.define_config_group_param('pool', 'name', 'string', 'Pool Name', writable=False)
        self.define_config_group_param('pool', 'health', 'string', 'Health', writable=False)
        self.define_config_group_param('pool', 'dedupratio', 'string', 'Dedupe ratio', writable=False)

        self.define_config_group_param('pool', 'capacity', 'string', 'Percentage filled', writable=False)
        self.define_config_group_param('pool', 'allocated', 'string', 'Allocated space', writable=False)
        self.define_config_group_param('pool', 'free', 'string', 'Free space', writable=False)
        self.define_config_group_param('pool', 'size', 'string', 'Total space', writable=False)

    def ui_getgroup_pool(self, key):
        '''
        This is the backend method for getting keys.
        @param key: The key to get the value of.
        @type key: str
        @return: The key's value
        @rtype: arbitrary
        '''
        obj = self._get_pool()
        return obj.properties.get(key)

    def ui_setgroup_pool(self, key, value):
        '''
        This is the backend method for setting keys.
        @param key: The key to set the value of.
        @type key: str
        @param value: The key's value
        @type value: arbitrary
        '''
        obj = self._get_pool()
        return obj.properties.set(key, value)


class StorageNode(AutomagicNode):
    """
    Child Creationism (Teach it to 'em young)
    """

    def ui_command_create_snapshot(self, name):
        '''
        create - Creates a snapshot
        '''
        parent = self._get_pool()
        #pool = self._get_pool()
        cls = Snapshot
        name = clean_name(name)

        obj_name = os.path.join(parent.name, name)
        obj = cls(name=obj_name)
        if obj.exists():
            raise ZfsError("Object '%s' already exists", name)
        obj.create()
        return True

    def ui_command_destroy(self, confirm=False):
        obj = self.obj
        if not obj.exists():
            raise ZfsError("Object '%s' does not exist", obj)
        if confirm == 'True':
            confirm = True
        obj.destroy(confirm=confirm)
        return True

    def ui_command_rename(self, new):
        path = self.obj.path(len=-1)
        path.append(clean_name(new))
        new = os.path.join(*path)
        obj = self.obj
        if not obj.exists():
            raise ZfsError("Object '%s' does not exist", obj)
        obj.rename(new)
        return True


class DatasetNode(StorageNode, PoolPropsMixIn, DatasetPropsMixIn):
    def _get_pool(self):
        return self.obj.get_pool()

    def _get_dataset(self):
        return self.obj

    def __init__(self, dataset):
        #self.obj = Pool(name=pool)
        super(DatasetNode, self).__init__()
        PoolPropsMixIn.__init__(self)
        DatasetPropsMixIn.__init__(self)

    def summary(self):
        # TODO Check disk usage percentage, generic self.obj.errors/warnings
        # interface perhaps?
        return (capfirst(self.obj.type), True)


from .device import Device, Devices


class RwDevices(Devices):
    _base_filter = {
        'is_readonly': False,
        'is_mounted': False,
        'path__notlambda': lambda v: v.startswith('zd') or v.startswith('drbd') or v.startswith('zram'),
    }


from solarsan.cli.events import CliEvent, RawInputEvent


class PoolsNode(AutomagicNode):
    def ui_children_factory_pool_list(self):
        return [p for p in Pool.list(ret=dict, ret_obj=False)]

    def ui_children_factory_pool(self, name):
        return PoolNode(name)

    def ui_command_ls_available_devices(self):
        # TODO This won't work.
        devices = RwDevices()
        return list(devices)

    def ui_command_create(self, name):
        #return self.service._conn.root.raw_input()
        return RawInputEvent.__call__
        #raise NotImplementedError


class VolumeNode(DatasetNode):
    def __init__(self, volume):
        self.obj = Volume(name=volume)
        super(VolumeNode, self).__init__(volume)

    def ui_children_factory_resource_dict(self):
        ret = {}
        try:
            # TODO Look up by device path
            res = DrbdResource.objects.get(name=self.obj.basename)
            peer_hostname = res.remote.hostname
            ret['replicated resource with %s' % peer_hostname] = dict(name=self.obj.basename)
        except DrbdResource.DoesNotExist:
            pass
        return ret

    def ui_children_factory_resource(self, name):
            return ResourceNode(name)

    def summary(self):
        return ('%s %s/%s' % (
            capfirst(self.obj.type),
            str(self.obj.properties['used']),
            str(self.obj.properties['volsize']),
        ), True)


class PoolNode(StorageNode, PoolPropsMixIn, DatasetPropsMixIn):
    def summary(self):
        return ('%s usage=%s/%s; health=%s' % (
            capfirst(self.obj.type),
            str(self.obj.properties['alloc']),
            str(self.obj.properties['size']),
            str(self.obj.properties['health']),
        ), self.obj.is_healthy())

    def _get_dataset(self):
        return self.obj.get_filesystem()

    def _get_pool(self):
        return self.obj

    def __init__(self, pool):
        self.obj = Pool(name=pool)
        StorageNode.__init__(self)
        PoolPropsMixIn.__init__(self)
        DatasetPropsMixIn.__init__(self)

    def ui_command_usage(self):
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

    def ui_command_create_volume(self, name, size):
        '''
        create - Creates a volume
        '''
        #parent = self._get_filesystem()
        parent = self._get_pool()
        cls = Volume
        name = clean_name(name)

        obj_name = os.path.join(parent.name, name)
        obj = cls(name=obj_name)
        if obj.exists():
            raise ZfsError("Object '%s' already exists", name)
        obj.create(size)
        return True

    def ui_children_factory_volume_dict(self):
        ret = {}
        for vol in self.obj.volumes():
            display_name = vol.basename

            ## TODO Keep track of this shit better, this is stupid.
            #try:
            #    res = DrbdResource.objects.get(name=display_name)
            #    peer_hostname = res.remote.hostname
            #    display_name = '%s (replicated resource with %s)' % (display_name, peer_hostname)
            #except DrbdResource.DoesNotExist:
            #    pass

            ret[display_name] = dict(name=vol.name)
        return ret

    def ui_children_factory_volume(self, name):
        return VolumeNode(name)

    """
    Devices
    """

    def ui_command_lsdevices(self):
        return self.obj.devices()

    ui_command_lsd = ui_command_lsdevices

    #def add_device(self, path, type='disk'):
    #    logging.error("TODO")

    #def replace_device(self, old, new):
    #    logging.error("TODO")

    """
    Status
    """

    def ui_command_iostat(self, capture_length=2):
        return self.obj.iostat(capture_length=capture_length)

    def ui_command_status(self):
        return self.obj.status()

    def ui_command_health(self):
        return str(self.obj.properties['health'])

    def ui_command_clear(self):
        return self.obj.clear()

    """
    Import/Export
    """

    def ui_command_import_(self):
        return self.obj.import_()

    def ui_command_export(self):
        return self.obj.export()

    """
    Cluster
    """

    ## TODO Attribute
    #def ui_command_is_clustered(self):
    #    return self.obj.is_clustered


#from solarsan.exceptions import ZfsError
from solarsan.cli.backend import AutomagicNode
from solarsan.storage.drbd import DrbdResource


class ResourcesNode(AutomagicNode):
    def ui_children_factory_resource_list(self):
        return [res.name for res in DrbdResource.objects.all()]

    def ui_children_factory_resource(self, name):
        return ResourceNode(name)

    def ui_command_create(self, name=None):
        # TODO Create Floating IP wizard
        raise NotImplemented


class ResourceNode(AutomagicNode):
    def __init__(self, name, display_name=None):
        if display_name:
            self.display_name = display_name
        self.obj = DrbdResource.objects.get(name=name)
        super(ResourceNode, self).__init__()

    def summary(self):
        return ('%s %s %s' % (self.obj.connection_state,
                              self.obj.role,
                              self.obj.disk_state,
                              ), True)
