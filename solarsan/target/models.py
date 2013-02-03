
from solarsan.core import logger
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from .utils import generate_wwn, is_valid_wwn
from . import scstadmin
from storage.drbd import DrbdResource


class Target(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    meta = {'abstract': True}
    #meta = {'allow_inheritance': True}

    name = m.StringField()
    luns = m.ListField()
    #initiators = m.ListField()
    is_enabled = m.BooleanField()

    def enumerate_luns(self):
        return enumerate(self.luns)

    @property
    def is_enabled_int(self):
        return int(self.is_enabled)


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

    def add_target(self):
        scstadmin.add_target(self.name, self.driver)

    def rem_target(self):
        scstadmin.rem_target(self.name, self.driver)

    def disable_target(self):
        scstadmin.disable_target(self.name, self.driver)

    def enable_target(self):
        scstadmin.enable_target(self.name, self.driver)

    def open_devs(self):
        ress = {}
        for res in DrbdResource.objects.filter(role='Primary'):
            ress[res.name] = res

        for lun in self.luns:
            if lun not in ress:
                logger.info('Target "%s" luns are not all available.', self.name)
                return False

        logger.info('Target "%s" luns are available.', self.name)

        for lun in self.luns:
            scstadmin.open_dev(lun, 'vdisk_blockio', filename=ress[lun].device)

    def close_devs(self):
        ress = {}
        for res in DrbdResource.objects.filter(role='Primary'):
            ress[res.name] = res

        for lun in self.luns:
            if lun not in ress:
                logger.info('Target "%s" luns are not all available.', self.name)
                return False

        logger.info('Target "%s" luns are available.', self.name)

        for lun in self.luns:
            scstadmin.close_dev(lun, 'vdisk_blockio')


class SRPTarget(Target):
    #meta = {'allow_inheritance': True}
    pass
