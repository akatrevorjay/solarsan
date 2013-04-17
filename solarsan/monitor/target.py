
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer
from solarsan.target.models import iSCSITarget
from .resource import ResourceSecondary
import random
import weakref
from .resource import get_resource
#from .peer import get_peer


"""
Target Manager
"""


class TargetsCheck(Event):
    """Check for new Targets"""


class TargetManager(Component):
    check_every = 300.0

    def __init__(self):
        super(TargetManager, self).__init__()
        self.monitors = {}

        self.targets_check()

        self._check_timer = Timer(self.check_every,
                                  TargetsCheck(),
                                  persist=True,
                                  ).register(self)

    def targets_check(self):
        uuids = []
        for tgt in iSCSITarget.objects.all():
            self.add_target(tgt)
            uuids.append(tgt.uuid)
        for uuid in self.monitors.keys():
            if uuid not in uuids:
                self.monitors[uuid].unregister()
                self.monitors.pop(uuid)

    def add_target(self, tgt):
        if tgt.uuid in self.monitors:
            return
        self.monitors[tgt.uuid] = TargetMonitor(tgt.uuid).register(self)


class TargetStart(Event):
    """Start Target"""


class TargetStartTry(Event):
    """Try to Start Target"""


class TargetStop(Event):
    """Stop Target"""


class TargetStarted(Event):
    """Start Target"""


class TargetStopped(Event):
    """Stop Target"""


def get_target(uuid):
    return iSCSITarget.objects.get(uuid=uuid)


class TargetMonitor(Component):
    uuid = None

    def __init__(self, uuid):
        self.uuid = uuid
        super(TargetMonitor, self).__init__()

        target = self.target
        logger.info('Monitoring Target "%s".', target.name)

        # TODO Check if target is active, check if all resources are primary
        # for target, if not stop ourselves.
        if target.enabled:
            logger.warning('Target "%s" is currently active upon startup.', target.name)
            #self.fire_this(TargetStart())
            self.fire_this(TargetStarted())
        elif target.added:
            logger.warning('Target "%s" currently exists upon startup.', target.name)

    def get_target(self):
        return get_target(self.uuid)

    _target = None

    @property
    def target(self):
        target = None
        if self._target:
            target = self._target()
        if target is None:
            try:
                target = self.get_target()
            except target.DoesNotExist:
                logger.error('Target with uuid=%s does not exist anymore', self.uuid)
                self.unregister()
            self._target = weakref.ref(target)
        return target

    def get_event(self, event):
        event.args.insert(0, self.uuid)
        return event

    def fire_this(self, event):
        event.args.insert(0, self.uuid)
        return self.fire(event)

    # When a Peer that we split this target with failsover,
    # become primary for the target.
    # ^ This should already be handled
    #   by resourcemonitor, the exception is when monitor is started while peer
    #   is already dead. In that case, peer_failover will work.
    #def peer_failover(self, peer):
    #    for dev in self.target.devices:
    #        if dev.remote.uuid != peer.uuid:
    #            continue
    #
    #        # Makes them fight over it. Hmm.
    #        self._check_lun_timer = Timer(10.0 + random.randrange(2, 8), self.get_event(TargetStart())).register(self)
    #        return True

    def resource_role_change(self, uuid, role):
        res = get_resource(uuid)
        target = self.target
        for dev in target.devices:
            if dev.pk == res.pk:
                logger.info('Target "%s": ' + 'Member Resource "%s" has become primary.',
                            target.name, res.name)
                #if not hasattr(self, '_start_try_timer'):
                #    self.fire_this(TargetStart())
                self.fire_this(TargetStart())
                return

    def resource_secondary_pre(self, uuid):
        res = get_resource(uuid)
        target = self.target
        for dev in target.devices:
            if dev.pk == res.pk:
                logger.info('Target "%s": Member Resource "%s" wants to become secondary. Trying to ' +
                            'deconfigure quickly enough so it can.', target.name, res.name)
                self.fire_this(TargetStop())
                #self.log(logger.warning, 'Member Resource "%s" is trying to become secondary while ' +
                #               'being part of an active target.',
                #               res.name)

    def target_start(self, uuid):
        if uuid != self.uuid:
            return
        if hasattr(self, '_start_try_timer'):
            return
        target = self.target
        #self.log(logger.info, 'Starting..')
        #self.fire_this(TargetStartTry())

        #if hasattr(self, '_start_try_timer'):
        #    self._start_try_timer.unregister()
        #    delattr(self, '_start_try_timer')

        retry_in = 8.0 + random.randrange(2, 8)
        logger.warning('Target "%s": ' + 'Trying to start in %ds.', target.name, retry_in)
        self._start_try_timer = Timer(retry_in, self.get_event(TargetStartTry())).register(self)

    def target_start_try(self, uuid, attempt=0):
        if uuid != self.uuid:
            return
        target = self.target

        def remove_start_try_timer():
            if hasattr(self, '_start_try_timer'):
                self._start_try_timer.unregister()
                delattr(self, '_start_try_timer')

        missing_devices = []
        for dev in target.devices:
            # Reload device from database as it may of changed since we last
            # retrieved it.
            dev.reload()
            if not dev.role == 'Primary':
                missing_devices.append(dev.name)
            if dev.remote_role == 'Primary':
                logger.error('Target "%s": ' + 'Cannot start because remote is primary on Resource "%s".',
                             target.name, dev.name)

                remove_start_try_timer()
                return

        if missing_devices:
            logger.error('Target "%s": ' + 'Cannot start because some luns are not available: "%s".',
                         target.name, missing_devices)

            # TODO Need to do a random choice between both when they are both
            # detined to be king.
            if attempt > 1 and len(missing_devices) < len(target.devices):
                if random.randrange(0, 9999) < 5000:
                    for dev in target.devices:
                        if dev.role == 'Primary':
                            self.fire(ResourceSecondary(dev))

            remove_start_try_timer()
            retry_in = 8.0 + random.randrange(2, 8)
            logger.warning('Target "%s": ' + 'Retrying to start in %ds.', target.name, retry_in)
            self._start_try_timer = Timer(retry_in, self.get_event(TargetStartTry(attempt=attempt + 1))).register(self)
            return

        logger.info('Target "%s": Can now start!', target.name)
        try:
            target.start()
            logger.info('Target "%s" started.', target.name)
        except Exception, e:
            logger.error('Target "%s": ' + 'Could not start: %s', target.name, e.message)
            remove_start_try_timer()
            return False
        remove_start_try_timer()
        self.fire_this(TargetStarted())
        return True

    def target_started(self, uuid):
        if uuid != self.uuid:
            return

    def target_stop(self, uuid):
        if uuid != self.uuid:
            return
        target = self.target

        logger.info('Target "%s": Can now stop!', target.name)
        target.stop()
        self.fire_this(TargetStopped())
