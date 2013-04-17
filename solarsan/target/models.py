
from solarsan import logging, signals
logger = logging.getLogger(__name__)
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.ha.models import FloatingIP
from .utils import generate_wwn, is_valid_wwn
from . import scstadmin
from solarsan.storage.volume import Volume
#from solarsan.storage.drbd import DrbdResource
from uuid import uuid4
import os


"""
Backstore
"""


class Backstore(ReprMixIn, m.Document):
    _repr_vars = ['name']
    meta = dict(allow_inheritance=True)

    name = m.StringField()          # Name
    handler = 'vdisk_blockio'       # Device handler

    #t10_dev_id = m.IntField()      # SCST will auto create this

    @property
    def active(self):
        return scstadmin.does_device_exist(self.name)

    is_active = active

    def start(self, target=None, group=None):
        logger.debug('Starting backstore %s for Group %s Target %s', self, group, target)
        return self.open()

    def stop(self, target=None, group=None):
        logger.debug('Stopping backstore %s for Group %s Target %s', self, group, target)

        # TODO What about a backstore that's used in multiple groups or targets?
        #return self.close()

    def open(self):
        if not self.is_active:
            logger.debug('Opening backstore device %s', self)
            scstadmin.open_dev(self.name, self.handler, filename=self.device)
        return True

    def close(self):
        if self.is_active:
            logger.debug('Closing backstore device %s', self)
            scstadmin.close_dev(self.name, self.handler, force=True)
        return True

    def resync_size(self):
        if self.is_active:
            logger.debug('Resyncing backstore device %s', self)
            scstadmin.resync_dev(self.device)
        return True

    def __unicode__(self):
        return self.__repr__()

    def save(self, *args, **kwargs):
        if not self.name:
            self.name = self.get_default_name()
        super(Backstore, self).save(*args, **kwargs)


class VolumeBackstore(Backstore):
    _repr_vars = ['name', 'volume_name']
    volume_name = m.StringField()

    def get_default_name(self):
        name = unicode(self.volume_name)
        name.replace('/', '__')
        return name

    @property
    def pool(self):
        return self.volume_name.split('\/', 1)[0]

    @property
    def volume_basename(self):
        return os.path.basename(self.volume_name)

    @property
    def volume(self):
        if self._volume_name:
            return Volume(self._volume_name)

    @volume.setter
    def volume(self, value):
        if not isinstance(value, Volume):
            raise ValueError('Not a Volume object: "%s".')
        if not value.exists():
            raise ValueError('Volume "%s" does not exist.')
        self.volume_name = value.name

    @property
    def device(self):
        return '/dev/zvol/%s' % self.volume_name


class DrbdResourceBackstore(Backstore):
    _repr_vars = ['name', 'resource']
    #resource = m.ReferenceField(DrbdResource)
    resource = m.GenericReferenceField()

    def get_default_name(self):
        name = unicode(self.resource.name)
        return name

    @property
    def volume(self):
        return Volume(self.resource.local.volume_full_name)

    @property
    def device(self):
        return self.resource.device


"""
Initiators
"""


#class Initiator(m.EmbeddedDocument):
#    wwn = m.StringField()
#    allowed = m.BooleanField()
#
#    def __unicode__(self):
#        return self.__repr__()


#class iSCSIInitiator(Initiator):
#    pass


#class SRPInitiator(Initiator):
#    pass


"""
Acl
"""


class Acl(ReprMixIn, m.EmbeddedDocument):
    allow = m.ListField()
    deny = m.ListField()
    #default_allow = m.BooleanField()

    def start(self, target=None, group=None):
        logger.debug('Starting Acl %s for Group %s for Target %s', self, group, target)
        for initiator in self.allow:
            scstadmin.add_init(initiator, target.driver, target.name, group.name)

    def stop(self, target=None, group=None):
        logger.debug('Stopping Acl %s for Group %s for Target %s', self, group, target)
        #for initiator in self.allow:
        #    scstadmin.rem_init(initiator, target.driver, target.name, group.name)
        scstadmin.clear_inits(target.driver, target.name, group.name)

    def __unicode__(self):
        return self.__repr__()


"""
Group
"""


class PortalGroup(ReprMixIn, m.EmbeddedDocument):
    _repr_vars = ['name']
    name = m.StringField()
    #luns = m.ListField(m.EmbeddedDocumentField(Lun))
    luns = m.ListField(m.ReferenceField(Backstore, dbref=False))
    acl = m.EmbeddedDocumentField(Acl)

    def __init__(self, *args, **kwargs):
        super(PortalGroup, self).__init__(*args, **kwargs)

        if not self.acl:
            self.acl = Acl()

    @property
    def _target(self):
        return self._instance

    def start(self, target=None):
        if not target:
            target = self._target
        logger.debug('Starting Group %s for Target %s', self, target)

        self._add_group(target=target)
        self._add_acl(target=target)
        self._add_luns(target=target)

        return True

    def _add_group(self, target=None):
        if not scstadmin.does_target_ini_group_exist(target.name, target.driver, self.name):
            logger.debug('Adding group %s for Target %s', self, target)
            scstadmin.add_group(self.name, target.driver, target.name)
        return True

    def _add_acl(self, target=None):
        return self.acl.start(target=target, group=self)

    def _add_luns(self, target=None):
        # Add luns
        for lun, backstore in enumerate(self.luns):
            lun += 1

            if not backstore.is_active:
                logger.debug('Starting backstore %s for Group %s Target %s', backstore, self, target)
                backstore.start(target=target, group=self)

            if not scstadmin.does_target_lun_exist(target.name, target.driver, lun, self.name):
                logger.debug('Adding lun %d with backstore %s for Group %s Target %s', lun, backstore, self, target)
                # Definition: scstadmin.add_lun(lun, driver, target, device, group=None, **attributes)
                scstadmin.add_lun(lun, target.driver, target.name, backstore.name, self.name)

    def stop(self, target=None):
        if not target:
            target = self._target
        logger.debug('Stopping Group %s for Target %s', self, target)

        self._rem_luns(target=target)
        self._rem_acl(target=target)
        self._rem_group(target=target)

        return True

    def _rem_luns(self, target=None):
        for lun, backstore in enumerate(self.luns):
            lun += 1

            if scstadmin.does_target_lun_exist(target.name, target.driver, lun):
                logger.debug('Removing lun %d with backstore %s for Group %s Target %s', lun, backstore, self, target)
                scstadmin.rem_lun(lun, target.driver, target.name, self.name)

            if backstore.is_active:
                logger.debug('Stopping backstore %s for Group %s Target %s', backstore, self, target)
                backstore.stop(target=target, group=self)

        # Clear out luns for group, to be safe
        scstadmin.clear_luns(target.driver, target.name, self.name)

    def _rem_acl(self, target=None):
        logger.debug('Removing Acl %s for Group %s for Target %s', self.acl, self, target)
        return self.acl.stop(target=target, group=self)

    def _rem_group(self, target=None):
        logger.debug('Removing Group %s for Target %s', self, target)
        return scstadmin.del_group(self.name, target.driver, target.name)

    def __unicode__(self):
        return self.__repr__()


"""
Targets
"""


class Target(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    meta = dict(abstract=True)

    class signals:
        pre_start = signals.pre_start
        post_start = signals.post_start
        pre_stop = signals.pre_stop
        post_stop = signals.post_stop

    name = m.StringField()
    floating_ip = m.ReferenceField(FloatingIP, dbref=False)
    uuid = m.UUIDField(binary=False)

    #devices = m.ListField(m.GenericReferenceField())
    #luns = m.ListField(m.EmbeddedDocumentField(Lun))
    groups = m.ListField(m.EmbeddedDocumentField(PortalGroup))
    #groups = m.ListField(m.GenericReferenceField())

    _dev_handler = 'vdisk_blockio'

    def start(self):
        logger.info('Starting Target %s', self)
        self.signals.pre_start.send(self)

        self._add_target()
        #self._add_devices()
        self.enabled = True

        for group in self.groups:
            group.start(target=self)

        self.signals.post_start.send(self)

    def stop(self):
        logger.info('Stopping Target %s', self)
        self.signals.pre_stop.send(self)

        if self.added:
            for group in self.groups:
                group.stop(target=self)

            self.enabled = False
            #self._rem_devices()
            self._rem_target()

        self.signals.post_stop.send(self)

    def save(self, *args, **kwargs):
        if not self.uuid:
            self.uuid = uuid4()
        super(Target, self).save(*args, **kwargs)

    @property
    def added(self):
        return scstadmin.does_target_exist(self.name, self.driver)

    @added.setter
    def added(self, value):
        if value is True:
            if not self.added:
                self._add_target()
        else:
            if self.added:
                self._rem_target()

    is_added = added

    def _add_target(self):
        logger.debug('Adding Target %s', self)
        if not self.is_added:
            scstadmin.add_target(self.name, self.driver)

    def _rem_target(self):
        logger.debug('Removing Target %s', self)
        if self.is_added:
            scstadmin.rem_target(self.name, self.driver)

    @property
    def enabled(self):
        return scstadmin.is_target_enabled(self.name, self.driver)

    @enabled.setter
    def enabled(self, value):
        if not self.added:
            return
        if value:
            self._enable_target()
        else:
            self._disable_target()

    is_enabled = enabled

    def _enable_target(self):
        logger.debug('Enabling Target %s', self)
        if not self.enabled:
            scstadmin.enable_target(self.name, self.driver)

    def _disable_target(self):
        logger.debug('Disabling Target %s', self)
        if self.enabled:
            scstadmin.disable_target(self.name, self.driver)

    def __unicode__(self):
        return self.__repr__()


@signals.post_start.connect
def _on_post_start(self):
    if issubclass(self.__class__, Target):
        # TODO What if the floating IP is part of many targets?
        if self.floating_ip:  # and not self.floating_ip.is_active:
            self.floating_ip.ifup()


@signals.pre_stop.connect
def _on_pre_stop(self):
    if issubclass(self.__class__, Target):
        # TODO What if the floating IP is part of many targets?
        if self.floating_ip:  # and self.floating_ip.is_active:
            self.floating_ip.ifdown()


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
