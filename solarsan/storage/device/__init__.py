
#from solarsan.core import logger
from solarsan.exceptions import DeviceHandlerNotFound
import os
from solarsan.utils.queryset import QuerySet

from .os_linux import backend


class DeviceSet(QuerySet):
    # This will be set later after Device class is created
    _wrap_objects = None

    #def _get_objs(self):
    #    wrapper = getattr(self, '_wrap_objects', None)
    #    devs = self._get_raw_objs()
    #    if wrapper:
    #        devs = [wrapper(d) for d in devs]
    #    return devs

    #def _get_raw_objs(self):
    #    #return backend.get_devices()
    #    #return list(Drives().all())
    #    #return list(
    #    return backend.get_devices()

    def __init__(self, *args, **kwargs):
        #if 'devices' in kwargs:
        #    kwargs['objects'] = kwargs.pop('devices')
        super(DeviceSet, self).__init__(*args, **kwargs)

    def _device_check(self):
        pass


class DeviceQuerySet(DeviceSet):
    def _get_objs(self):
        wrapper = getattr(self, '_wrap_objects', None)
        devs = self._get_raw_objs()
        if wrapper:
            devs = [wrapper(d) for d in devs]
        return devs

    def _get_raw_objs(self):
        return backend.get_devices()


class BaseDevice(backend.BaseDevice):
    """Device object
    """
    path = None
    _mirrorable = False
    _backend_device = None
    _zpool_create_modifier = None

    @classmethod
    def _get_backend_device(self, device):
        if isinstance(device, backend.RawDevice):
            return device
        elif isinstance(device, BaseDevice):
            return device._backend_device
        elif isinstance(device, basestring):
            device = backend.guess_device_path(device)
            if device:
                return backend.get_device_by_path(device)
        #raise Exception("Could not get backend device for '%s'" % device)

    def __init__(self, device):
        self._backend_device = self._get_backend_device(device)
        if self._backend_device:
            self.path = self.path_by_id(basename=True)
        else:
            self._exists = False
            self.path = device

    def __repr__(self):
        append = ''

        append += "'%s'" % self.path
        if not self.exists():
            append += ', exists=False'

        return '<%s(%s)>' % (self.__class__.__name__, append)

    def exists(self):
        if self._backend_device:
            return True
        else:
            return False

    def _zpool_arg(self):
        #assert self.is_drive or self.is_partition
        #assert not self.is_mounted
        #assert not self.is_partitioned
        assert self.exists()
        return self.path_by_id()

    def path_by_id(self, basename=False):
        paths = self.paths()
        ret = None

        path_by_uuid = None
        path_by_path = None
        path_by_id = None
        path_short = None
        for x, path in enumerate(paths):
            if path.startswith('/dev/disk/by-uuid/'):
                path_by_uuid = path
            elif path.startswith('/dev/disk/by-path/'):
                path_by_path = path
            elif path.startswith('/dev/disk/by-id/'):
                path_by_id = path
            if not path_short or len(path_short) > len(path):
                path_short = path

        for i in [path_by_uuid, path_by_id, path_by_path, path_short, paths[0]]:
            if i:
                ret = i
                break

        if basename:
            ret = os.path.basename(ret)
        return ret


class Mirror(DeviceSet):
    """Mirrored device object
    """
    _mirrorable = True

    def __init__(self, *args, **kwargs):
        self._objs = []
        super(Mirror, self).__init__(*args, **kwargs)

    #def _get_objs(self):
    #    return []

    def _zpool_args(self):
        assert len(self) % 2 == 0
        modifiers = self._zpool_create_modifiers
        return modifiers + [dev._zpool_arg() for dev in self._objs]

    @property
    def _zpool_create_modifiers(self):
        ret = []
        device_class = self._device_class
        if device_class:
            #ret.extend(device_class._zpool_create_modifiers)
            modifier = device_class._zpool_create_modifier
            if modifier:
                ret.append(modifier)
        ret.append('mirror')
        return ret

    @property
    def _device_class(self):
        if self:
            return self[0].__class__

    def _device_check(self, v):
        device_class = self._device_class
        if device_class:
            success = True

            meth = getattr(v, '_mirrorable_with', None)
            if meth:
                if not meth(device_class):
                    success = False
            elif not isinstance(v, device_class):
                success = False

            if not success:
                raise ValueError("Device '%s' is not mirrorable with '%s'" % (v, device_class))

        mirrorable = getattr(v, '_mirrorable', None)
        if not mirrorable:
            raise ValueError("Device is not mirrorable")

        if v in self:
            raise ValueError("Cannot mirror the same device multiple times")

    def __setitem__(self, k, v):
        self._device_check(v)
        return super(Mirror, self).__setitem__(k, v)

    def append(self, v, _device_check=True):
        if _device_check:
            self._device_check(v)
        return super(Mirror, self).append(v)

    def __add__(self, other):
        return self.append(other)


class _MirrorableDeviceMixin(object):
    """_mirrorable device mixin
    """
    _mirrorable = True

    def __add__(self, other):
        if isinstance(other, (self.__class__, Mirror)):
            return Mirror([self, other])


class Device(BaseDevice):
    def __new__(cls, backend_device, *args, **kwargs):
        backend_device = cls._get_backend_device(backend_device)
        #logger.debug("cls: %s backend_device=%s, args=%s kwargs=%s", cls, backend_device, args, kwargs)

        if backend_device:
            for subclass in cls._handlers():
                if subclass._supports_backend(backend_device):
                    return super(Device, cls).__new__(subclass, backend_device, *args, **kwargs)

        if getattr(cls, '_supports_backend', None):
            if not cls._supports_backend(backend_device):
                raise DeviceHandlerNotFound(backend_device)
        return super(Device, cls).__new__(cls, backend_device, *args, **kwargs)

    @classmethod
    def _handlers(cls):
        for c in cls.__subclasses__():
        #for c in Device.__subclasses__():
            #meth = getattr(subclass, '_handlers', None)
            #if meth:
            #    for sub in meth():
            #        yield sub
            meth = getattr(c, '_supports_backend', None)
            if meth:
                yield c


class Disk(_MirrorableDeviceMixin, Device):
    """Disk device object
    """
    @classmethod
    def _supports_backend(cls, backend_device):
        return backend_device.DeviceIsDrive

    @classmethod
    def _mirrorable_with(cls, device_class):
        if device_class in [Disk, Partition]:
            return True


class Partition(_MirrorableDeviceMixin, Device):
    """Partiton device object
    """
    @classmethod
    def _supports_backend(cls, backend_device):
        return backend_device.DeviceIsPartition

    @classmethod
    def _mirrorable_with(cls, device_class):
        if device_class in [Disk, Partition]:
            return True


#class PoolDevice(object):
#    def __init__(self, device):
#        if isinstance(device, PoolDevice):
#            device = device._device
#        self._device = device
#        super(PoolDevice, self).__init__()


#class PoolDeviceSet(object):
#    pass


class Cache(_MirrorableDeviceMixin, Device):
    """Cache device object
    """
    _zpool_create_modifier = 'cache'


class Log(_MirrorableDeviceMixin, Device):
    """Log device object
    """
    _zpool_create_modifier = 'log'


class Spare(Device):
    """Spare device object
    """
    _zpool_create_modifier = 'spare'


#DeviceQuerySet._wrap_objects = Device
DeviceSet._wrap_objects = Device
#DeviceQuerySet._wrap_objects = backend.Device
#Devices = Devices()
#Drives = Drives()


class Devices(DeviceQuerySet):
    pass


class ZfsDevices(DeviceQuerySet):
    _base_filter = {
        'is_zfs_member': True
    }


class Disks(DeviceQuerySet):
    _base_filter = {
        'is_drive': True,
        #'path__notstartswith': ['zd', 'drbd', 'zram'],
        'path__notlambda': lambda v: v.startswith('zd') or v.startswith('drbd') or v.startswith('zram'),
    }


class Partitions(DeviceQuerySet):
    _base_filter = {
        'is_partition': True
    }
