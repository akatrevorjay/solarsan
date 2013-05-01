
from solarsan import logging, signals
logger = logging.getLogger(__name__)
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.ha.models import FloatingIP
from .rtsutils import generate_wwn, is_valid_wwn
from .scst import scstsys

from solarsan.storage.volume import Volume
#from solarsan.storage.drbd import DrbdResource
from uuid import uuid4
import os


"""
Backstore
"""


def dictattrs(**attrs):
    return '; '.join(['%s=%s' % (k, v) for k, v in attrs.iteritems()])


def get_handler(handler):
    return getattr(scstsys.handlers, handler, None)


def get_driver(driver):
    return getattr(scstsys.targets, driver, None)


def get_target(driver, target):
    driver = get_driver(driver)
    if not driver:
        return
    return getattr(driver, target, None)


def get_ini_group(driver, target, group):
    tgt = get_target(driver, target)
    if not tgt:
        return
    return getattr(tgt.ini_groups, group, None)


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
Acl
"""


class Acl(ReprMixIn, m.EmbeddedDocument):
    initiators = m.ListField(m.StringField())

    # TODO insecure option
    #insecure = m.BooleanField()

    # TODO chap auth
    #chap = m.BooleanField()
    #chap_user = m.StringField()
    #chap_pass = m.StringField()

    def start(self, target=None, group=None):
        logger.debug('Starting %s for %s for %s', self, group, target)

        ini_group = get_ini_group(target.driver, target.name, group.name)
        if ini_group:
            for initiator in self.initiators:
                if initiator in ini_group.initiators:
                    continue
                logger.debug('Adding intiator %s for %s for %s for %s',
                             initiator, self, group, target)
                ini_group.initiators.mgmt = 'add %s' % initiator

    def stop(self, target=None, group=None):
        logger.debug('Stopping %s for %s for %s', self, group, target)
        ini_group = get_ini_group(target.driver, target.name, group.name)
        if ini_group:
            for initiator in self.initiators:
                if initiator not in ini_group.initiators:
                    continue
                logger.debug('Removing initiator %s for %s for %s for %s',
                             initiator, self, group, target)
                ini_group.initiators.mgmt = 'del %s' % initiator
            logger.debug('Clearing all initiators for %s for %s for %s',
                         self, group, target)
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

        self._del_luns(target=target)
        self._del_acl(target=target)
        self._del_group(target=target)

        return True

    def _del_luns(self, target=None):
        ini_group = get_ini_group(target.driver, target.name, self.name)

        for lun, backstore in enumerate(self.luns):
            lun += 1

            if ini_group and str(lun) in ini_group.luns:
                logger.debug('Removing lun %d with backstore %s for %s for %s', lun, backstore, self, target)
                ini_group.luns.mgmt = 'del {0}'.format(lun)

            if backstore.is_active:
                logger.debug('Stopping backstore %s for Group %s Target %s', backstore, self, target)
                backstore.stop(target=target, group=self)

        if self.is_active(target=target):
            # Clear out luns for group, to be safe
            ini_group.luns.mgmt = 'clear'

    def _del_acl(self, target=None):
        logger.debug('Removing Acl %s for Group %s for Target %s', self.acl, self, target)
        return self.acl.stop(target=target, group=self)

    def _del_group(self, target=None):
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

    # Initiator groups
    groups = m.ListField(m.EmbeddedDocumentField(PortalGroup))

    recent_denied_initiators = m.ListField(m.StringField())
    #recent_allowed_initiators = m.ListField(m.StringField())

    def start(self):
        logger.info('Starting Target %s', self)
        self.signals.pre_start.send(self)

        self._add_target()

        for group in self.groups:
            group.start(target=self)

        self.enabled = True
        self.driver_enabled = True

        self.signals.post_start.send(self)

    def stop(self):
        logger.info('Stopping Target %s', self)
        self.signals.pre_stop.send(self)

        if self.added:
            for group in self.groups:
                group.stop(target=self)

            self.enabled = False
            self._del_target()
            self.driver_enabled = False

        self.signals.post_stop.send(self)

    def save(self, *args, **kwargs):
        if not self.uuid:
            self.uuid = uuid4()
        super(Target, self).save(*args, **kwargs)

    @property
    def added(self):
        drv = get_driver(self.driver)
        return self.name in drv

    @added.setter
    def added(self, value):
        if value is True:
            if not self.added:
                self._add_target()
        else:
            if self.added:
                self._del_target()

    is_added = added

    """
    The following target driver attributes available: IncomingUser, OutgoingUser
    The following target attributes available: IncomingUser, OutgoingUser, allowed_portal
    """

    @property
    def parameters(self):
        return dictattrs(
            #rel_tgt_id=randrange(1000, 3000),
            #read_only=int(False),
        )

    @property
    def attributes(self):
        return dict(
            #IncomingUser='test testtesttesttest',
            #OutgoingUser='test testtesttesttest',
        )

    def _add_target(self):
        logger.debug('Adding %s', self)
        if not self.is_added:
            drv = get_driver(self.driver)

            parameters = self.parameters
            if parameters:
                logger.debug('Parameters for %s: %s', self, parameters)
            drv.mgmt = 'add_target {0.name} {1}'.format(self, parameters)

            self._add_target_attrs()

    def _add_target_attrs(self):
        #logger.debug('Adding %s attributes.', self)
        attributes = self.attributes
        if attributes:
            logger.debug('Adding attribute to %s: %s', self, attributes)
            for k, v in self.attributes.iteritems():
                drv.mgmt = 'add_target_attribute {0.name} {1} {2}'.format(self, k, v)

    def _del_target(self):
        logger.debug('Removing Target %s', self)
        if self.is_added:
            self._del_target_attrs()
            drv = get_driver(self.driver)
            drv.mgmt = 'del_target {0.name}'.format(self)

    def _del_target_attrs(self):
        logger.debug('Removing %s attributes.', self)
        attributes = self.attributes
        if attributes:
            logger.debug('Removing attribute to %s: %s', self, attributes)
            for k, v in self.attributes.iteritems():
                drv.mgmt = 'del_target_attribute {0.name} {1} {2}'.format(self, k, v)

    @property
    def enabled(self):
        tgt = get_target(self.driver, self.name)
        if not tgt:
            return False
        return bool(tgt.enabled)

    @enabled.setter
    def enabled(self, value):
        if not self.added:
            return
        if value == self.enabled:
            return
        tgt = get_target(self.driver, self.name)
        if value:
            logger.debug('Enabling Target %s', self)
        else:
            logger.debug('Disabling Target %s', self)
        value = int(bool(value))
        if tgt.enabled != value:
            tgt.enabled = value

    is_enabled = enabled

    # TODO This does not belong here, it belongs in a Driver class.
    @property
    def driver_enabled(self):
        drv = get_driver(self.driver)
        if not drv:
            return False
        return bool(int(drv.enabled))

    # TODO This does not belong here, it belongs in a Driver class.
    @driver_enabled.setter
    def driver_enabled(self, value):
        drv = get_driver(self.driver)
        if not drv:
            return False

        value = int(bool(value))
        if value == self.driver_enabled:
            return

        for root, dirs, files in os.walk(drv._path_):
            break
        drv_subdir_count = len(dirs)

        if not value and drv_subdir_count:
            logger.info('Not disabling driver %s as it is currently in use.', self.driver)
            return
        elif not value:
            logger.info('Disabling driver %s as it is no longer in use.', self.driver)
        else:
            logger.info('Enabling driver %s.', self.driver)
        drv.enabled = value

    def __unicode__(self):
        return self.__repr__()

    @classmethod
    def search_hard(cls, **kwargs):
        if not kwargs:
            return
        for subcls in cls.__subclasses__():
            try:
                qs = subcls.objects.get(**kwargs)
                return qs
            except subcls.DoesNotExist:
                pass


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
