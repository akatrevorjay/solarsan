
from .base import BaseServiceConfigNode


class StorageNode(BaseServiceConfigNode):
    def __init__(self, parent, obj):
        self._obj = obj
        obj_path = obj.split('/')
        super(StorageNode, self).__init__(obj_path[-1], parent)

        #self.define_config_group_param(
        #    'property', 'compress', 'string',
        #    'If on, enables compression. This increases performance and lowers disk usage.')

        #self.define_config_group_param(
        #    'property', 'dedup', 'string',
        #    'If on, enables deduplication. Should only be used with very specific workloads.')

        #self.define_config_group_param(
        #    'property', 'atime', 'string',
        #    'If on, enables updates of file access times. This hurts performance, and is rarely wanted.')

        #self.define_config_group_param(
        #    'statistic', 'compressratio', 'string',
        #    'Compression ratio for this dataset and children.')

        #self.define_config_group_param(
        #    'statistic', 'dedupratio', 'string',
        #    'Deduplication ratio for this dataset and children.')

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

    """
    Child Creationism (Teach it to them young)
    """

    #def ui_command_create_filesystem(self, name):
    #    '''
    #    create - Creates a Filesystem
    #    '''
    #    self()

    def ui_command_create_volume(self, name, size):
        '''
        create - Creates a volume
        '''
        self()

    def ui_command_create_snapshot(self, name):
        '''
        create - Creates a snapshot
        '''
        self()

    def ui_command_destroy(self, confirm=False):
        self(confirm=confirm)

    def ui_command_rename(self, new):
        self(new)

    """
    Properties
    """

    #POOL_PROPERTIES = []

    #def ui_getgroup_property(self, property):
    #    '''
    #    This is the backend method for getting propertys.
    #    @param property: The property to get the value of.
    #    @type property: str
    #    @return: The property's value
    #    @rtype: arbitrary
    #    '''
    #    if property in self.POOL_PROPERTIES:
    #        obj = self.get_pool()
    #    else:
    #        obj = self.get_filesystem()
    #    return str(obj.properties[property])

    #def ui_setgroup_property(self, property, value):
    #    '''
    #    This is the backend method for setting propertys.
    #    @param property: The property to set the value of.
    #    @type property: str
    #    @param value: The property's value
    #    @type value: arbitrary
    #    '''
    #    if property in self.POOL_PROPERTIES:
    #        obj = self.get_pool()
    #    else:
    #        obj = self.get_filesystem()
    #    obj.properties[property] = value

    #POOL_STATISTICS = ['dedupratio']

    #def ui_getgroup_statistic(self, statistic):
    #    '''
    #    This is the backend method for getting statistics.
    #    @param statistic: The statistic to get the value of.
    #    @type statistic: str
    #    @return: The statistic's value
    #    @rtype: arbitrary
    #    '''
    #    if statistic in self.POOL_STATISTICS:
    #        obj = self.get_pool()
    #    else:
    #        obj = self.get_filesystem()

    #    return str(obj.properties[statistic])

    #def ui_setgroup_statistic(self, statistic, value):
    #    '''
    #    This is the backend method for setting statistics.
    #    @param statistic: The statistic to set the value of.
    #    @type statistic: str
    #    @param value: The statistic's value
    #    @type value: arbitrary
    #    '''
    #    #self.obj.properties[statistic] = value
    #    return None


#def add_child_dataset(self, child):
#    if child.type == 'filesystem':
#        Dataset(self, child)
#    elif child.type == 'volume':
#        Dataset(self, child)
#    elif child.type == 'snapshot':
#        Dataset(self, child)


#class StorageNodeChildType(ConfigNode):
#    def __init__(self, parent, child_type):
#        self.child_type = child_type
#        super(StorageNodeChildType, self).__init__('%ss' % child_type, parent)


class Dataset(StorageNode):
    def __init__(self, parent, dataset):
        super(Dataset, self).__init__(parent, dataset)

    #def ui_type_blah(self):
    #    pass

    #def summary(self):
    #    # TODO Check disk usage percentage, generic self.obj.errors/warnings
    #    # interface perhaps?
    #    return (self.obj.type, True)


class Pool(StorageNode):
    help_intro = '''
                 STORAGE POOL
                 ============
                 Storage Pools are dataset containers, they contain datasets such as a filesystem or a volume.

                 Think of it as a giant swimming pool for your data.
                 That is, except some odd fellow replaced your water with solid state drives and rotating platters.
                 '''

    def __init__(self, parent, pool):
        super(Pool, self).__init__(parent, pool)

    #def summary(self):
    #    return (self.obj.health, self.obj.is_healthy())

    def ui_command_usage(self):
        self()

    """
    Devices
    """

    def ui_command_lsdevices(self):
        self()

    #def ui_command_add_device(self, path, type='disk'):
    #    logging.error("TODO")

    #def ui_command_replace_device(self, old, new):
    #    logging.error("TODO")

    """
    Status
    """

    def ui_command_iostat(self, capture_length=2):
        self()

    def ui_command_status(self):
        self()

    def ui_command_health(self):
        self()

    def ui_command_clear(self):
        self()

    """
    Import/Export
    """

    def ui_command_import_(self):
        self()

    def ui_command_export(self):
        self()

    """
    Cluster
    """

    ## TODO Attribute
    #def ui_command_is_clustered(self):
    #    self()


class Storage(BaseServiceConfigNode):
    def __init__(self, parent):
        super(Storage, self).__init__(None, parent)

        for pool in self.service.pool_list():
            Pool(self, pool)

    def ui_command_create_pool(self, name):
        '''
        create - Creates a storage Pool
        '''
        self()

    def ui_command_lsscsi(self):
        '''
        lsscsi - list SCSI devices (or hosts) and their attributes
        '''
        self()

    def ui_command_df(self):
        '''
        df - report file system disk space usage
        '''
        self()

    def ui_command_lsblk(self):
        self()
