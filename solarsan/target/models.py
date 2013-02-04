
from solarsan.core import logger
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from .utils import generate_wwn, is_valid_wwn
from . import scstadmin
#from storage.drbd import DrbdResource
from ha.models import ActivePassiveIP


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

    floating_ip = m.ReferenceField(ActivePassiveIP, dbref=False)

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

    def _add_devices(self):
        for x, device in enumerate(self.devices):
            x += 1
            device.open()
            if not scstadmin.does_target_lun_exist(self.name, self.driver, x):
                scstadmin.add_lun(x, self.driver, self.name, device.name)

    def _remove_devices(self):
        for x, device in enumerate(self.devices):
            x += 1
            if scstadmin.does_target_lun_exist(self.name, self.driver, x):
                scstadmin.rem_lun(x, self.driver, self.name)
            device.close()

    def start(self):
        self._add_target()
        self._add_devices()
        self.enable_target()

    def stop(self):
        self.disable_target()
        self._remove_devices()
        self._remove_target()


class iSCSITarget(Target):
    #meta = {'allow_inheritance': True}
    driver = 'iscsi'

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
