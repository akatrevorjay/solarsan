
from solarsan import logging, signals
logger = logging.getLogger(__name__)
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.ha.models import FloatingIP
from .rtsutils import generate_wwn, is_valid_wwn
from . import scstadmin
from .scst import scstsys

from solarsan.storage.volume import Volume
#from solarsan.storage.drbd import DrbdResource
from uuid import uuid4
import os


"""
Backstore
"""


def get_handler(handler):
    return getattr(scstsys.handlers, handler, None)


def dictattrs(**attrs):
    return ';'.join(['%s=%s' % (k, v) for k, v in attrs.iteritems()])


class Backstore(ReprMixIn, m.Document):
    _repr_vars = ['name']
    meta = dict(allow_inheritance=True)

    name = m.StringField()          # Name
    handler = 'vdisk_blockio'       # Device handler

    #t10_dev_id = m.IntField()      # SCST will auto create this

    @property
    def active(self):
        return bool(str(self.name) in scstsys.devices)

    is_active = active

    @property
    def _hnd(self):
        return get_handler(self.handler)

    @property
    def _dev(self):
        return getattr(scstsys.devices, self.name, None)

    @property
    def attributes(self):
        return dictattrs(filename=self.resource.device)

    def start(self, target=None, group=None):
        logger.debug('Starting backstore %s for Group %s Target %s', self, group, target)
        return self.open()

    def stop(self, target=None, group=None):
        logger.debug('Stopping backstore %s for Group %s Target %s', self, group, target)

        # TODO What about a backstore that's used in multiple groups or targets?
        return self.close()

    def open(self):
        if not self.is_active:
            logger.debug('Opening backstore device %s', self)
            self._hnd.mgmt = 'add_device {0.name} {0.attributes}'.format(self)
        return True

    def close(self):
        if self.is_active:
            logger.debug('Closing backstore device %s', self)
            self._hnd.mgmt = 'del_device {0.name}'.format(self)
        return True

    def resync_size(self):
        if self.is_active:
            logger.debug('Resyncing backstore device %s', self)
            self._dev.resync_size = 0
        return True

    def __unicode__(self):
        return self.__repr__()

    def save(self, *args, **kwargs):
        if not self.name:
            self.name = self.get_default_name()
        super(Backstore, self).save(*args, **kwargs)

    @property
    def device(self):
        raise NotImplementedError

    def detach(self):
        pass

    def attach(self):
        pass


class VolumeBackstore(Backstore):
    _repr_vars = ['name', 'volume_name', 'device']
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

    def is_available(self):
        # TODO Should really check if it's RW if not self.is_active I suppose
        return os.path.exists(self.device)


class DrbdResourceBackstore(Backstore):
    _repr_vars = ['name', 'resource', 'device']
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

    def is_available(self):
        # TODO Is this needed?
        self.resource.reload()
        return self.resource.role == 'Primary'

    def detach(self):
        self.resource.reload()
        if self.resource.role == 'Primary':
            self.resource.local.service.secondary()

    def attach(self):
        self.resource.reload()
        if self.resource.role == 'Secondary':
            self.resource.local.service.primary()


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


def get_target(driver, target):
    driver = getattr(scstsys.targets, driver, None)
    if driver:
        return getattr(driver, target, None)


def get_ini_group(driver, target, group):
    tgt = get_target(driver, target)
    if tgt:
        return getattr(tgt.ini_groups, group, None)


class Acl(ReprMixIn, m.EmbeddedDocument):
    initiators = m.ListField(m.StringField())
    #allow = m.ListField()
    #insecure = m.BooleanField()

    #chap = m.BooleanField()
    #chap_user = m.StringField()
    #chap_pass = m.StringField()

    def start(self, target=None, group=None):
        logger.debug('Starting %s for %s for %s', self, group, target)

        # TODO insecure option
        # TODO chap auth

        ini_group = get_ini_group(target.driver, target.name, group.name)
        if ini_group:
            for initiator in self.initiators:
                logger.debug('Adding intiator %s for %s for %s for %s', initiator, self, group, target)
                ini_group.initiators.mgmt = 'add %s' % initiator

    def stop(self, target=None, group=None):
        logger.debug('Stopping %s for %s for %s', self, group, target)
        ini_group = get_ini_group(target.driver, target.name, group.name)
        if ini_group:
            for initiator in self.initiators:
                logger.debug('Removing initiator %s for %s for %s for %s', initiator, self, group, target)
                ini_group.initiators.mgmt = 'del %s' % initiator
            logger.debug('Clearing all initiators for %s for %s for %s', self, group, target)
            ini_group.initiators.mgmt = 'clear'

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

    def is_active(self, target=None):
        if not target:
            target = self._target
        tgt = get_target(target.driver, target.name)
        return tgt and self.name in tgt.ini_groups

    def start(self, target=None):
        if not target:
            target = self._target
        logger.debug('Starting Group %s for Target %s', self, target)

        self._add_group(target=target)
        self._add_acl(target=target)
        self._add_luns(target=target)

        return True

    def _add_group(self, target=None):
        if not self.is_active(target=target):
            logger.debug('Adding group %s for Target %s', self, target)
            tgt = get_target(target.driver, target.name)
            if not tgt:
                raise Exception('Could not get target {0.name}'.format(target))
            tgt.ini_groups.mgmt = 'create %s' % self.name
        return True

    def _add_acl(self, target=None):
        return self.acl.start(target=target, group=self)

    def _add_luns(self, target=None):
        ini_group = get_ini_group(target.driver, target.name, self.name)

        for lun, backstore in enumerate(self.luns):
            lun += 1

            if not backstore.is_active:
                logger.debug('Starting backstore %s for %s for %s', backstore, self, target)
                backstore.start(target=target, group=self)

            if not str(lun) in ini_group.luns:
                logger.debug('Adding lun %d with backstore %s for %s for %s', lun, backstore, self, target)
                """parameters: read_only"""
                ini_group.luns.mgmt = 'add {0.name} {1}'.format(backstore, lun)

    def stop(self, target=None):
        if not target:
            target = self._target
        logger.debug('Stopping Group %s for Target %s', self, target)

        self._rem_luns(target=target)
        self._rem_acl(target=target)
        self._rem_group(target=target)

        return True

    def _rem_luns(self, target=None):
        ini_group = get_ini_group(target.driver, target.name, self.name)

        for lun, backstore in enumerate(self.luns):
            lun += 1

            if str(lun) in ini_group.luns:
                logger.debug('Removing lun %d with backstore %s for %s for %s', lun, backstore, self, target)
                ini_group.luns.mgmt = 'del {0}'.format(lun)

            if backstore.is_active:
                logger.debug('Stopping backstore %s for Group %s Target %s', backstore, self, target)
                backstore.stop(target=target, group=self)

        if self.is_active(target=target):
            # Clear out luns for group, to be safe
            scstadmin.clear_luns(target.driver, target.name, self.name)

    def _rem_acl(self, target=None):
        logger.debug('Removing Acl %s for Group %s for Target %s', self.acl, self, target)
        return self.acl.stop(target=target, group=self)

    def _rem_group(self, target=None):
        if self.is_active(target=target):
            logger.debug('Removing Group %s for Target %s', self, target)
            tgt = get_target(target.driver, target.name)
            if not tgt:
                raise Exception('Could not get target {0.name}'.format(target))
            tgt.ini_groups.mgmt = 'del %s' % self.name

    def __unicode__(self):
        return self.__repr__()


"""
Targets
"""


class Target(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    meta = dict(allow_inheritance=True)

    class signals:
        start = signals.start
        pre_start = signals.pre_start
        post_start = signals.post_start

        stop = signals.stop
        pre_stop = signals.pre_stop
        post_stop = signals.post_stop

    def __init__(self, *args, **kwargs):
        super(Target, self).__init__(*args, **kwargs)

        #DrbdResource.signals.status_change.connect(self.on_drbd_status_change)

    def get_all_luns(self):
        for group in self.groups:
            for lun in group.luns:
                yield lun

    def get_all_lun_devices(self):
        devices = []
        for lun in self.get_all_luns():
            dev = lun.device
            if dev not in devices:
                devices.append(dev)
                yield dev

    def get_all_unavailable_luns(self):
        for lun in self.get_all_luns():
            if not lun.is_available():
                yield lun

    # TODO Temp hack
    @property
    def devices(self):
        for lun in self.get_all_luns():
            # TODO what about volume backstores?
            dev = lun.resource
            yield dev

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


@signals.start.connect
def _on_start(self, **kwargs):
    if issubclass(self.__class__, Target):
        return self.start()


@signals.stop.connect
def _on_stop(self, **kwargs):
    if issubclass(self.__class__, Target):
        return self.stop()


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
    driver = 'srpt'
