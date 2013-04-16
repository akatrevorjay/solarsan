
#from solarsan.core import logger
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from .utils import generate_wwn, is_valid_wwn
from . import scstadmin
#from solarsan.storage.drbd import DrbdResource
from solarsan.ha.models import FloatingIP
from uuid import uuid4
from blinker import signal


class Device(ReprMixIn, m.Document, CreatedModifiedDocMixIn):
    meta = {'abstract': True}
    #meta = {'allow_inheritance': True}
    name = m.StringField()
    handler = 'vdisk_blockio'

    @property
    def device(self):
        raise NotImplemented

    def open(self):
        if not scstadmin.does_device_exist(self.name):
            scstadmin.open_dev(self.name, self.handler, filename=self.device)

    def close(self):
        if scstadmin.does_device_exist(self.name):
            scstadmin.close_dev(self.name, self.handler, force=True)


class VolumeDevice(Device):
    pool = m.StringField()
    volume_name = m.StringField()

    @property
    def device(self):
        return '/dev/zvol/%s/%s' % (self.pool, self.volume)


class Group(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    meta = dict(abstract=True)


class PortalGroup(Group):
    pass


class DeviceGroup(Group):
    pass


#class ResourceDevice(Device):
#    resource = m.ReferenceField(DrbdResource, dbref=False)
#    #resource = m.GenericReferenceField()
#
#    @property
#    def device(self):
#        return self.resource.device


class Target(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    meta = {'abstract': True}

    name = m.StringField()
    devices = m.ListField(m.GenericReferenceField())
    #initiators = m.ListField()

    floating_ip = m.ReferenceField(FloatingIP, dbref=False)

    uuid = m.UUIDField(binary=False)

    def save(self, *args, **kwargs):
        if not self.uuid:
            self.uuid = uuid4()
        super(Target, self).save(*args, **kwargs)

    @property
    def is_target_added(self):
        return scstadmin.does_target_exist(self.name, self.driver)

    @property
    def is_target_enabled(self):
        return scstadmin.is_target_enabled(self.name, self.driver)

    def _add_target(self):
        if not self.is_target_added:
            scstadmin.add_target(self.name, self.driver)

    def _remove_target(self):
        if self.is_target_added:
            scstadmin.rem_target(self.name, self.driver)

    def disable_target(self):
        if self.is_target_enabled:
            scstadmin.disable_target(self.name, self.driver)

    def enable_target(self):
        if not self.is_target_enabled:
            scstadmin.enable_target(self.name, self.driver)

    _dev_handler = 'vdisk_blockio'

    def _open_device(self, name, device):
        if not scstadmin.does_device_exist(name):
            scstadmin.open_dev(name, self._dev_handler, filename=device)

    def _close_device(self, name):
        if scstadmin.does_device_exist(name):
            scstadmin.close_dev(name, self._dev_handler, force=True)

    def _add_devices(self):
        for x, device in enumerate(self.devices):
            x += 1
            self._open_device(device.name, device.device)
            if not scstadmin.does_target_lun_exist(self.name, self.driver, x):
                scstadmin.add_lun(x, self.driver, self.name, device.name)

    def _remove_devices(self):
        for x, device in enumerate(self.devices):
            x += 1
            if scstadmin.does_target_lun_exist(self.name, self.driver, x):
                scstadmin.rem_lun(x, self.driver, self.name)
            self._close_device(device.name)

    def start(self):
        self._add_target()
        self._add_devices()
        self.enable_target()

        global target_started
        target_started.send(self)

    def stop(self):
        self.disable_target()
        self._remove_devices()
        self._remove_target()

        global target_stopped
        target_stopped.send(self)


class iSCSITarget(Target):
    #meta = {'allow_inheritance': True}
    driver = 'iscsi'
    #portal_port = m.IntField()

    def generate_wwn(self, serial=None):
        self.name = generate_wwn('iqn')
        return True

    def save(self, *args, **kwargs):
        """Overrides save to ensure name is a valid iqn; generates one if None"""
        if self.name:
            if not is_valid_wwn('iqn', self.name):
                raise ValueError("The name '%s' is not a valid iqn" % self.name)
        else:
            self.generate_wwn()
        super(iSCSITarget, self).save(*args, **kwargs)


class SRPTarget(Target):
    #meta = {'allow_inheritance': True}
    driver = 'srpt'


#from mongoengine.signals import post_save
#post_save.connect(FloatingIP.on_target_post_save, sender=iSCSITarget)
#post_save.connect(FloatingIP.on_target_post_save, sender=SRPTarget)

target_started = signal('target_started')
target_stopped = signal('target_stopped')


def _fip_up_on_target_started(self):
    # TODO What if the floating IP is part of many targets?
    if self.floating_ip and not self.floating_ip.is_active:
        self.floating_ip.ifup()

target_started.connect(_fip_up_on_target_started)


def _fip_down_on_target_stopped(self):
    # TODO What if the floating IP is part of many targets?
    if self.floating_ip and self.floating_ip.is_active:
        self.floating_ip.ifdown()

target_stopped.connect(_fip_down_on_target_stopped)
