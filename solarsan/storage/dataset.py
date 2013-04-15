
from solarsan.utils import LoggedException
#import logging
import sh
from .base import BaseProperty, Base


class DatasetProperty(BaseProperty):
    pass


class DatasetProperties(object):
    """Storage Dataset Properties object
    """

    def __init__(self, parent):
        self._parent = parent

    def __getitem__(self, k):
        """Gets dataset property.

        dataset = Dataset('dpool/carp')
        dataset.properties['alloc']

        """
        try:
            return self._get(k)
        except sh.ErrorReturnCode_2:
            raise KeyError

    def __setitem__(self, k, v):
        """Sets dataset property.

        dataset = Dataset('dpool/carp')
        dataset.properties['readonly'] = 'on'

        """
        try:
            return self._set(k, v)
        except sh.ErrorReturnCode_2:
            raise ValueError

    def __iter__(self):
        # TODO yield
        return iter(self._get('all'))

    def dumps(self):
        ret = {}
        for p in self:
            ret[p.name] = p.value
        return ret

    def _get(self, *props, **kwargs):
        """Gets dataset property.

        dataset = Dataset('dpool/carp')
        dataset.properties._get('alloc', 'free')
        dataset.properties._get('all')

        """
        assert props

        source = kwargs.get('source')

        ret = []
        skip = 1

        cmd = sh.zfs.bake('get')
        nargs = []
        if source:
            nargs.extend(['-s', source])
        nargs.extend([','.join(props), self._parent.name])

        for line in cmd(*nargs):
            if skip > 0:
                skip -= 1
                continue
            line = line.rstrip("\n")
            (obj_name, name, value, source) = line.split(None, 3)
            ret.append(DatasetProperty(self, name, value, source))

        # If we only requested a single property from a single object that
        # isn't the magic word 'all', just return the value.
        if len(props) == 1 and len(ret) == len(props) and 'all' not in props:
            ret = ret[0]
        return ret

    def get(self, name, default=None, source=None):
        """Get dataset property"""
        try:
            prop = self._get(name, source=source)
            ret = prop.value
        except KeyError:
            ret = default
        return ret

    def _set(self, k, v, ignore=False):
        """Sets Dataset property.

        dataset = Dataset('dpool/carp')
        dataset.properties._set('readonly', 'on')

        """
        if ignore:
            return

        prop = None
        if isinstance(v, DatasetProperty):
            prop = v
            v = prop.value

        sh.zfs('set', '%s=%s' % (k, v), self._parent.name)

        if prop:
            prop.value = v

    def set(self, name, value):
        """Set Dataset property"""
        return self._set(name, value)

    # TODO Delete item == inherit property
    #def __delitem__(self, k):
    #    """Deletes dataset property.
    #    """
    #    try:
    #        return self._inherit(k)
    #    except sh.ErrorReturnCode_1:
    #        raise KeyError

    #def _inherit(self, k):
    #    """Inherits property from parents
    #    """


class Dataset(Base):
    """Base Dataset object
    """

    def __init__(self, name=None, **kwargs):
        super(Dataset, self).__init__(name, **kwargs)
        if name and getattr(self, 'name', None) is not name:
            self.name = name
        self._init(name, **kwargs)

    def _init(self, name, **kwargs):
        self.properties = DatasetProperties(self)

    def get_pool(self):
        from .pool import Pool
        return Pool(self.pool_name)

    #@property
    #def pool(self):
    #    """ Returns the matching Pool for this Dataset """
    #    return storage.all.Pool(name=self.path(0, 1))
    #    #return self._get_type('pool')(name=self.path(0, 1))

    #@property
    #def parent(self):
    #    """ Returns the parent of this Dataset """
    #    path = self.path()
    #    if len(path) == 1:
    #        return None
    #    return self._get_type('dataset')(name='/'.join(path[:-1]))

    # Good candidates to share code once again:
    #def exists
    #def destroy

    @property
    def basename(self):
        return self.path(-1)[0]

    @property
    def pool_name(self):
        name = self.path(0, 1)[0]
        if '@' in name:
            name = name.split('@', 1)[0]
        return name

    @classmethod
    def list(cls, args=None, skip=None, props=None, ret=None, ret_obj=True, type=None, pool=None):
        """Lists storage datasets.
        """
        if isinstance(args, basestring):
            args = [args]
        elif not args:
            args = []
        if not props:
            props = ['name']
        if not 'guid' in props:
            props.append('guid')
        if not 'type' in props:
            props.append('type')
        if not type:
            type = cls.__name__.lower()

        ret_type = ret or list
        if ret_type == list:
            ret = []
        elif ret_type == dict:
            ret = {}
        else:
            raise LoggedException("Invalid return object type '%s' specified", ret_type)

        # Generate command and execute, parse output
        cmd = sh.zfs.bake('list', '-o', ','.join(props), '-t', type)
        header = None
        for line in cmd(*args):
            line = line.rstrip("\n")
            if not header:
                header = line.lower().split()
                continue
            cols = dict(zip(header, line.split()))
            name = cols['name']

            if skip and skip == name:
                continue

            if pool and name.split('/')[0] != pool:
                continue

            if ret_obj:
                ## FIXME This hsould be handled in __init__ of document subclass me thinks
                objcls = cls._get_type(cols['type'])
                obj = objcls._get_obj(**cols)
                # TODO Update props as well?
            else:
                obj = cols

            if ret_type == dict or not ret_obj:
                ret[name] = obj
            elif ret_type == list:
                ret.append(obj)

        return ret

    @classmethod
    def _get_obj(cls, **kwargs):
        return cls(**kwargs)

    @classmethod
    def _get_type(cls, objtype):
        # TODO Find a better way
        from .glue import glue_get_type
        return glue_get_type(cls, objtype)
