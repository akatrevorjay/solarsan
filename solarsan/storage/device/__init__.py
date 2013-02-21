
from solarsan.core import logger
from solarsan.exceptions import DeviceHandlerNotFound
import os
from solarsan.utils.queryset import QuerySet

from .os_linux import backend


class DeviceQuerySet(QuerySet):
    # This will be set later after Device class is created
    _wrap_objects = None

    def _get_objs(self):
        wrapper = getattr(self, '_wrap_objects', None)
        devs = self._get_raw_objs()
        if wrapper:
            devs = [wrapper(d) for d in devs]
        return devs

    def _get_raw_objs(self):
        #return backend.get_devices()
        #return list(Drives().all())
        #return list(
        return backend.get_devices()

    def __init__(self, *args, **kwargs):
        if 'devices' in kwargs:
            kwargs['objects'] = kwargs.pop('devices')
        super(DeviceQuerySet, self).__init__(*args, **kwargs)

    def _device_check(self):
        pass


#class Device(object):
#    """Device object"""
#
#    def __init__(self, path):
#        self._parted = get_device_by_path(path)
#        self._udisks = get_device_by_path(path)
#    # TODO


#class Devices(DeviceQuerySet):
#    pass


#class Drives(DeviceQuerySet):
#    base_filter = {
#        'DeviceIsDrive': True,
#    }

#    ## TODO Drives should maybe not show volume devs?
#    #path_by_id = d.path_by_id()
#    #basepath = os.path.basename(path_by_id)
#    #if basepath.startswith('zd'):
#    #    continue


class Mirror(DeviceQuerySet):
    """Mirrored device object
    """
    _mirrorable = True

    def _zpool_args(self):
        assert len(self) % 2 == 0
        modifiers = self._zpool_create_modifiers
        return modifiers + [dev._zpool_arg() for dev in self.objects]

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
        if not device_class:
            return
        if not v.__class__ == device_class:
            raise ValueError("Cannot mirror different types of devices")
        if v in self.objects:
            raise ValueError("Cannot mirror the same device multiple times")

    def __setitem__(self, k, v):
        self._device_check(v)
        return super(Mirror, self).__setitem__(k, v)

    def append(self, v):
        self._device_check(v)
        return super(Mirror, self).append(v)

    def __add__(self, other):
        return self.append(other)


#class MyMeta(type):
#    def __new__(meta, name, bases, dct):
#        print '-----------------------------------'
#        print "Allocating memory for class", name
#        print meta
#        print bases
#        print dct
#        return super(MyMeta, meta).__new__(meta, name, bases, dct)
#
#    def __init__(cls, name, bases, dct):
#        print '-----------------------------------'
#        print "Initializing class", name
#        print cls
#        print bases
#        print dct
#        super(MyMeta, cls).__init__(name, bases, dct)


class BaseDevice(backend.BaseDevice):
    """Device object
    """
    #__metaclass__ = MyMeta

    path = None
    _mirrorable = False
    _backend_device = None
    _zpool_create_modifier = None

    #def __new__(cls, backend_device, *args, **kwargs):
    #    print "cls: %s backend_device=%s, args=%s kwargs=%s" % (cls, backend_device, args, kwargs)
    #    print backend_device.DeviceIsDrive
    #    obj = super(Device, cls).__new__(cls, backend_device, *args, **kwargs)
    #    return obj

    def __init__(self, arg):
        if isinstance(arg, backend.RawDevice):
            self._backend_device = arg
        elif isinstance(arg, BaseDevice):
            self._backend_device = arg._backend_device
        else:
            self._backend_device = backend.get_device_by_path(arg)
        self.path = self.path_by_id(basename=True)

    def __repr__(self):
        return "<%s('%s')>" % (self.__class__.__name__, self.path)

    def _zpool_arg(self):
        #assert self.is_drive or self.is_partition
        #assert not self.is_mounted
        #assert not self.is_partitioned
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


class __MirrorableDeviceMixin(object):
    """_mirrorable device mixin
    """
    _mirrorable = True

    def __add__(self, other):
        if isinstance(other, (self.__class__, Mirror)):
            return Mirror([self, other])


class Device(BaseDevice):
    def __new__(cls, backend_device, *args, **kwargs):
        #logger.debug("cls: %s backend_device=%s, args=%s kwargs=%s", cls, backend_device, args, kwargs)

        if isinstance(backend_device, BaseDevice):
            backend_device = backend_device._backend_device

        for subclass in cls._handlers():
            if subclass._supports_backend(backend_device):
                return super(Device, cls).__new__(subclass, backend_device, *args, **kwargs)

        if getattr(cls, '_supports_backend', None):
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


class Disk(__MirrorableDeviceMixin, Device):
    """Disk device object
    """
    @classmethod
    def _supports_backend(cls, backend_device):
        return backend_device.DeviceIsDrive


class Partition(__MirrorableDeviceMixin, Device):
    """Partiton device object
    """
    @classmethod
    def _supports_backend(cls, backend_device):
        return backend_device.DeviceIsPartition


class Cache(__MirrorableDeviceMixin, Device):
    """Cache device object
    """
    _zpool_create_modifier = 'cache'


class Log(__MirrorableDeviceMixin, Device):
    """Log device object
    """
    _zpool_create_modifier = 'log'


class Spare(Device):
    """Spare device object
    """
    _zpool_create_modifier = 'spare'


DeviceQuerySet._wrap_objects = Device
#DeviceQuerySet._wrap_objects = backend.Device
#Devices = Devices()
#Drives = Drives()


class Devices(DeviceQuerySet):
    pass


class Drives(DeviceQuerySet):

    #base_filter={
    #    # TODO This is for udisks, what attr fits here?
    #    'DeviceIsDrive': True,
    #}

    ## TODO Drives should maybe not show volume devs?
    #path_by_id = d.path_by_id()
    #basepath = os.path.basename(path_by_id)
    #if basepath.startswith('zd'):
    #    continue

    pass
