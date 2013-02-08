
from solarsan.core import logger
from circuits import Component, Event, Timer
from solarsan.target.models import iSCSITarget
import random


"""
Target Manager
"""


class TargetManager(Component):

    def __init__(self):
        super(TargetManager, self).__init__()
        #self.targets = {}
        self.monitors = {}

        for tgt in iSCSITarget.objects.all():
            #self.targets[tgt.name] = tgt
            self.monitors[tgt.name] = TargetMonitor(tgt).register(self)


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


class TargetMonitor(Component):
    def __init__(self, target):
        super(TargetMonitor, self).__init__()
        self.target = target
        logger.info('Monitoring Target "%s".', self.target.name)

    def log(self, level, message, *args, **kwargs):
        prepend = 'Target "%s": ' % self.target.name
        level(prepend + message, *args, **kwargs)

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
    #        self._check_lun_timer = Timer(10.0 + random.randrange(2, 8), TargetStart(self.target)).register(self)
    #        return True

    def resource_role_change(self, res, role):
        for dev in self.target.devices:
            if dev.pk == res.pk:
                self.log(logger.info, 'Member Resource "%s" has become primary.', res.name)
                #if not hasattr(self, '_start_try_timer'):
                #    self.fire(TargetStart(self.target))
                self.fire(TargetStart(self.target))
                return

    def resource_secondary_pre(self, res):
        for dev in self.target.devices:
            if dev.pk == res.pk:
                self.log(logger.info, 'Member Resource "%s" wants to become secondary. Trying to ' +
                         'deconfigure quickly enough so it can.', res.name)
                self.fire(TargetStop(self.target))
                #self.log(logger.warning, 'Member Resource "%s" is trying to become secondary while ' +
                #               'being part of an active target.',
                #               res.name)

    def target_start(self, target):
        if target.pk != self.target.pk:
            return
        if hasattr(self, '_start_try_timer'):
            return
        #self.log(logger.info, 'Starting..')
        #self.fire(TargetStartTry(self.target))

        #if hasattr(self, '_start_try_timer'):
        #    self._start_try_timer.unregister()
        #    delattr(self, '_start_try_timer')

        retry_in = 8.0 + random.randrange(2, 8)
        self.log(logger.warning, 'Trying to start in %ds.', retry_in)
        self._start_try_timer = Timer(retry_in, TargetStartTry(self.target)).register(self)

    def target_start_try(self, target, attempt=0):
        if target.pk != self.target.pk:
            return

        def remove_start_try_timer():
            if hasattr(self, '_start_try_timer'):
                self._start_try_timer.unregister()
                delattr(self, '_start_try_timer')

        missing_devices = []
        for dev in self.target.devices:
            # Reload device from database as it may of changed since we last
            # retrieved it.
            dev.reload()
            if not dev.role == 'Primary':
                missing_devices.append(dev.name)
            if dev.remote_role == 'Primary':
                self.log(logger.error, 'Cannot start because remote is primary on Resource "%s".',
                         dev.name)

                remove_start_try_timer()
                return

        if missing_devices:
            self.log(logger.error, 'Cannot start because some luns are not available: "%s".',
                     missing_devices)

            # TODO Need to do a random choice between both when they are both
            # detined to be king.
            if attempt > 1 and len(missing_devices) < len(self.target.devices):
                if random.randrange(0, 9999) < 5000:
                    for dev in self.target.devices:
                        if dev.role == 'Primary':
                            self.fire(ResourceSecondary(dev))

            remove_start_try_timer()
            retry_in = 8.0 + random.randrange(2, 8)
            self.log(logger.warning, 'Retrying to start in %ds.', retry_in)
            self._start_try_timer = Timer(retry_in, TargetStartTry(self.target, attempt=attempt + 1)).register(self)
            return

        self.log(logger.info, 'Can now start!')
        try:
            self.target.start()
        except Exception, e:
            self.log(logger.error, 'Could not start: %s', e.message)
            remove_start_try_timer()
            return False
        remove_start_try_timer()
        self.fire(TargetStarted(self.target))
        return True

    def target_started(self, target):
        if target.pk != self.target.pk:
            return

    def target_stop(self, target):
        if target.pk != self.target.pk:
            return
        self.log(logger.info, 'Can now stop!')
        self.target.stop()
        self.fire(TargetStopped(self.target))


