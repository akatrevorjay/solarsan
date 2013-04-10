
from solarsan.exceptions import ZfsError
from solarsan.storage.utils import clean_name
from django.template.defaultfilters import capfirst
import os

from solarsan.cli.backend import AutomagicNode
from .drbd import ResourceNode

from ..drbd import DrbdResource
from ..pool import Pool
#from .filesystem import Filesystem
from ..volume import Volume
from ..snapshot import Snapshot


class StorageNode(AutomagicNode):
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
            return self.obj.get_filesystem()
        elif self.obj.type == 'filesystem':
            return self.obj

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


class DatasetNode(StorageNode):
    def __init__(self, dataset):
        #self.obj = Pool(name=pool)
        super(DatasetNode, self).__init__()

    #def ui_type_blah(self):
    #    pass

    def summary(self):
        # TODO Check disk usage percentage, generic self.obj.errors/warnings
        # interface perhaps?
        return (capfirst(self.obj.type), True)


class PoolsNode(AutomagicNode):
    def ui_children_factory_pool_list(self):
        return [p for p in Pool.list(ret=dict, ret_obj=False)]

    def ui_children_factory_pool(self, name):
        return PoolNode(name)

    def ui_command_create_pool(self, name):
        # TODO Create Pool Wizard
        raise NotImplemented


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
            ret['(replicated resource with %s)' % peer_hostname] = dict(name=self.obj.basename)
        except DrbdResource.DoesNotExist:
            pass
        return ret

    def ui_children_factory_resource(self, name):
            return ResourceNode(name)

    def summary(self):
        return ('%s %s/%s' % (capfirst(self.obj.type),
                              str(self.obj.properties['used']),
                              str(self.obj.properties['volsize']),
                              ), True)


class PoolNode(StorageNode):
    def __init__(self, pool):
        self.obj = Pool(name=pool)
        super(PoolNode, self).__init__()

    def summary(self):
        return ('%s %s/%s' % (capfirst(self.obj.type),
                              str(self.obj.properties['alloc']),
                              str(self.obj.properties['size']),
                              ), self.obj.is_healthy())

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
