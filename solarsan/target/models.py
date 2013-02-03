
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from .utils import generate_wwn, is_valid_wwn


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
    pass
