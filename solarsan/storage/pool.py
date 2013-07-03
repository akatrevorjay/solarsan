
#import logging
import sh
#from collections import defaultdict, OrderedDict
#import re
from solarsan.exceptions import ZfsError
from datetime import datetime
from collections import OrderedDict

'''
from analytics.cube import CubeAnalytics
from pypercube.expression import EventExpression, MetricExpression, CompoundMetricExpression
from pypercube.expression import Sum, Min, Max, Median, Distinct
'''

from .base import Base, BaseProperty
from . import device
from .parsers.pool import zpool_status_parse2


'''
class PoolAnalytics(CubeAnalytics):
    """Storage Pool Analytics object
    """
    def __init__(self, parent):
        self._parent = parent

    def _get_event_expr(self, f, **kwargs):
        #return EventExpression('pool_iostat', f).eq('pool', self._parent.name).gt(f, 0)
        return EventExpression('pool_iostat', f).eq('pool', self._parent.name)

    def _get_metric_expr(self, f, **kwargs):
        e = kwargs.get('event_expr', self._get_event_expr(f, **kwargs))
        return Median(e)
        #return Sum(e)

    def iops(self, **kwargs):
        return self._render('iops_read', 'iops_write', **kwargs)

    def bandwidth(self, **kwargs):
        return self._render('bandwidth_read', 'bandwidth_write', **kwargs)

    def usage(self, **kwargs):
        return self._render('alloc', 'free', **kwargs)
'''


class PoolProperty(BaseProperty):
    """Storage Pool Property object
    """
    pass


class PoolProperties(object):
    """Storage Pool Properties object
    """

    def __init__(self, parent):
        self._parent = parent

    def __getitem__(self, k):
        """Gets pool property.

        pool = Pool('dpool')
        pool.properties['alloc']

        """
        return self._get(k)

    def __setitem__(self, k, v):
        """Sets pool property.

        pool = Pool('dpool')
        pool.properties['readonly'] = 'on'

        """
        return self._set(k, v)

    def __iter__(self):
        # TODO yield
        return iter(self._get('all'))

    def dumps(self):
        ret = {}
        for p in self:
            ret[p.name] = p.value
        return ret

    def _get(self, *props):
        """Gets pool property.

        pool = Pool('dpool')
        pool.properties._get('alloc', 'free')
        pool.properties._get('all')

        """
        assert props

        ret = []
        skip = 1
        try:
            out = sh.zpool('get', ','.join(props), self._parent.name)
        except sh.ErrorReturnCode_2:
            raise KeyError
        for line in out:
            if skip > 0:
                skip -= 1
                continue
            line = line.rstrip("\n")
            (obj_name, name, value, source) = line.split(None, 3)
            ret.append(PoolProperty(self, name, value, source))

        # If we only requested a single property from a single object that
        # isn't the magic word 'all', just return the value.
        if len(props) == 1 and len(ret) == len(props) and 'all' not in props:
            ret = ret[0]
        return ret

    def get(self, name, default=None):
        try:
            ret = self._get(name).value
        except KeyError:
            ret = default
        return ret

    def _set(self, k, v, ignore=False):
        """Sets pool property.

        pool = Pool('dpool')
        pool.properties._set('readonly', 'on')

        """
        if ignore:
            return

        prop = None
        if isinstance(v, PoolProperty):
            prop = v
            v = prop.value

        try:
            sh.zpool('set', '%s=%s' % (k, v), self._parent.name)
        except sh.ErrorReturnCode_2:
            raise ValueError

        if prop:
            prop.value = v

    def set(self, name, value):
        return self._set(name, value)

    # TODO Delete item == inherit property
    #def __delitem__(self, k):
    #    """Deletes pool property.
    #    """
    #    try:
    #        return self._inherit(k)
    #    except sh.ErrorReturnCode_1:
    #        raise KeyError

    #def _inherit(self, k):
    #    """Inherits property from parents
    #    """


class Pool(Base):
    """Storage Pool object
    """
    type = 'pool'

    def __init__(self, name, **kwargs):
        super(Pool, self).__init__(name, **kwargs)
        if name and getattr(self, 'name', None) is not name:
            self.name = name
        self._init(name, **kwargs)

    def _init(self, name, **kwargs):
        self.properties = PoolProperties(self)
        #self.analytics = PoolAnalytics(self)

    """
    General
    """

    def exists(self):
        """Checks if pool exists.

        pool = Pool('dpool')
        pool.exists()

        """
        try:
            sh.zpool('list', self.name)
        except sh.ErrorReturnCode_1:
            return False
        return True

    def is_healthy(self):
        return unicode(self.properties['health']) == u'ONLINE'

    def is_degraded(self):
        return unicode(self.properties['health']) == u'DEGRADED'

    #@property
    def health_state_str(self):
        if self.is_healthy:
            return 'success'
        elif self.is_degraded:
            return 'warning'
        else:
            return 'error'

    @classmethod
    def _devices_to_args(cls, *devices):
        devs = OrderedDict({
            None: [],
            'log': [],
            'cache': [],
        })

        for dev in devices:
            modifier = getattr(dev, '_zpool_create_modifier', None)
            if getattr(dev, '_zpool_args', None):
                devs[modifier].append(dev._zpool_args())
            else:
                devs[modifier].append(dev._zpool_arg())

        args = []
        for k, vs in devs.items():
            if not vs:
                continue
            if k:
                args.append(k)
            for v in vs:
                if isinstance(v, basestring):
                    args.append(v)
                else:
                    args.extend(v)

        return args

    def get_filesystem(self):
        from .filesystem import Filesystem
        return Filesystem(self.name)

    def create(self, *devices, **kwargs):
        """Creates storage pool.

        pool = Pool('dpool')
        pool.create(Mirror(Disk('sda'), Disk('sdb')),
            Disk('sda') + Disk('sdb'),
            Log('sda') + Log('sdb'),
            Cache('sde'),
            Cache('sdf'),
            )

        """
        dry_run = kwargs.pop('dry_run', False)
        if dry_run:
            cmd = sh.echo.bake('zpool', 'create', self.name)
        else:
            cmd = sh.zpool.bake('create', self.name)

        args = self._devices_to_args(*devices)

        #try:
        if dry_run:
            print cmd(*args)
        else:
            cmd(*args)
        #except rv.ErrorReturnCode_1:
        #    return False
        return True

    def clear(self):
        """Clears any errors on storage pool.

        pool = Pool('dpool')
        pool.clear()

        """
        sh.zpool('clear', self.name)
        return True

    def import_(self):
        """Imports storage pool.

        pool = Pool('dpool')
        pool.import_()

        """
        sh.zpool('import', self.name)
        return True

    def export(self):
        """Exports storage pool.

        pool = Pool('dpool')
        pool.export()

        """
        sh.zpool('export', self.name)
        return True

    def destroy(self, confirm=False):
        """Destroys storage pool.

        pool = Pool('dpool')
        pool.destroy()

        """
        if confirm is not True:
            raise ZfsError('Destroy of storage pool requires confirm=True')
        sh.zpool('destroy', self.name)
        return True

    def status(self, devices=False):
        """Returns status of storage pool.

        pool = Pool('dpool')
        pool.status()

        """
        out = sh.zpool('status', '-v', self.name).stdout
        ret = zpool_status_parse2(from_string=out)
        ret = ret[self.name]
        if not devices:
            ret.pop('devices', None)
        return ret

    def devices(self):
        """Returns devices of storage pool.

        pool = Pool('dpool')
        pool.devices()

        """
        return self.status(devices=True).get('devices')

    def get_devices_alt(self):
        """Returns devices of storage pool, but doesn't require the pool to be imported.

        Caveat: It also gets any devices that WERE in a pool of the same name that have not been
        used otherwise yet. TODO use guid as well to avoid this.

        pool = Pool('dpool')
        pool.get_devices_alt()

        """
        # TODO Keep track of pool names and pool guids, use both here.
        return list(device.ZfsDevices(id_label=self.name))

    def iostat(self, capture_length=30):
        """Returns iostat of storage pool.

        pool = Pool('dpool')
        pool.iostat()

        """
        timestamp = None
        skip_past_dashline = False
        for line in sh.zpool('iostat', '-T', 'u', self.name, capture_length, 2):
            line = line.rstrip("\n")

            # Got a timestamp
            if line.isdigit():
                # If this is our first record, skip till we get the header seperator
                if not timestamp:
                    skip_past_dashline = True
                # TODO TZify the timestamp
                timestamp = datetime.fromtimestamp(int(line))
                continue

            # If we haven't gotten the dashline yet, wait till the line after it
            if skip_past_dashline:
                if line.startswith('-----'):
                    skip_past_dashline = False
                continue
            # Either way, let's not worry about them
            if line.startswith('-----'):
                continue

            # If somehow we got here without a timestamp, something is probably wrong.
            if not timestamp:
                raise ZfsError("Got unexpected input from zpool iostat: %s", line)

            # Parse iostats output
            j = {}
            j['timestamp'] = timestamp
            (j['name'],
             j['alloc'], j['free'],
             j['iops_read'], j['iops_write'],
             j['bandwidth_read'], j['bandwidth_write']) = line.strip().split()
            # Cut the crap
            j.pop('name')
            return j

    @classmethod
    def list(cls, args=None, skip=None, props=None, ret=None, ret_obj=True):
        """Lists storage pools.
        """
        nargs = []
        if args:
            nargs.extend(*args)

        if not props:
            props = []
        for i in ['name', 'guid']:
            if not i in props:
                props.append(i)

        ret_type = ret or list
        if ret_type == list:
            ret = []
        elif ret_type == dict:
            ret = {}
        else:
            raise ZfsError("Invalid return object type '%s' specified", ret_type)

        # Generate command and execute, parse output
        cmd = sh.zpool.bake('list', '-o', ','.join(props))
        header = None
        for line in cmd(*nargs):
            line = line.rstrip("\n")
            if not header:
                header = line.lower().split()
                continue
            cols = dict(zip(header, line.split()))
            name = cols['name']

            if skip and skip == name:
                continue

            if ret_obj:
                ## FIXME This hsould be handled in __init__ of document subclass me thinks
                obj = cls._get_obj(**cols)
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

    """
    Children
    """

    def volumes(self, **kwargs):
        # TODO Fix this, nasty
        from solarsan.storage.volume import Volume
        kwargs['pool'] = self.name
        return Volume.list(**kwargs)

    def children(self):
        return self.get_volumes()

    """
    Device
    """

    def add(self, device):
        """Grow pool by adding new device.

        pool = Pool('dpool')
        pool.add(
            Disk('sda') + Disk('sdb'),
            )

        """
        cmd = sh.zpool.bake('add', self.name)

        args = []
        for dev in [device]:
            if getattr(dev, '_zpool_args'):
                args.extend(dev._zpool_args())
            else:
                args.append(dev._zpool_arg())

        cmd(*args)
        return True

    def remove(self, device):
        """Removes device from pool.

        pool = Pool('dpool')
        pool.remove(Disk('sdc'))

        """
        cmd = sh.zpool.bake('remove', self.name)

        args = []
        for dev in [device]:
            args.append(dev._zpool_arg())

        cmd(*args)
        return True

    def attach(self, device, new_device):
        """Attaches new device to existing device, creating a device mirror.

        pool = Pool('dpool')
        pool.attach(Disk('sdb'), Disk('sdc'))

        """
        cmd = sh.zpool.bake('attach', self.name)

        args = []
        for dev in [device, new_device]:
            args.append(dev._zpool_arg())

        cmd(*args)
        return True

    def detach(self, device):
        """Detaches existing device from an existing device mirror.

        pool = Pool('dpool')
        pool.detach('sdb')

        """
        cmd = sh.zpool.bake('detach', self.name)

        args = []
        for dev in [device]:
            args.append(dev._zpool_arg())

        cmd(*args)
        return True

    def replace(self, device, new_device):
        """Replacees device with new device.

        pool = Pool('dpool')
        pool.replace(Disk('sdb'), Disk('sdc'))

        """
        cmd = sh.zpool.bake('replace', self.name)

        args = []
        for dev in [device, new_device]:
            args.append(dev._zpool_arg())

        cmd(*args)
        return True

    def upgrade(self):
        """Upgrades storage pool version.

        pool = Pool('dpool')
        pool.upgrade()

        """
        sh.zpool('upgrade', self.name)
        return True
