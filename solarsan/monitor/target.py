
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer, handler
from solarsan.target.models import Target, iSCSITarget, SRPTarget
from .resource import ResourceSecondary
import random
import weakref
from .resource import get_resource
#from .peer import get_peer


"""
Target Manager
"""


class TargetManager(Component):
    channel = 'target'

    def __init__(self):
        super(TargetManager, self).__init__()
        self.monitors = {}

    def managers_check(self):
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
        #self.monitors[tgt.uuid] = TargetMonitor(tgt.uuid, channel-'target-%s' % tgt.uuid).register(self)
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
    channel = 'target'
    uuid = None

    def __init__(self, uuid, channel=channel):
        self.uuid = uuid
        super(TargetMonitor, self).__init__(channel=channel)

        target = self.target
        logger.info('Monitoring Target %s.', target)

    def started(self, component):
        target = self.target

        # TODO Check if target is active, check if all resources are primary
        # for target, if not stop ourselves.
        if target.enabled:
            logger.warning('%s is currently active upon startup.', self.log_prepend)
            #self.fire_this(TargetStart())
            self.fire_this(TargetStarted())
            #self.fire(TargetStarted())
            #event = TargetStarted()
            #self.fire(event)
            #yield None
            #logger.info('Back in started with event=%s', event)
        elif target.added:
            logger.warning('%s is currently added upon startup.', self.log_prepend)

    def get_target(self):
        return get_target(self.uuid)

    _target = None

    @property
    def target(self):
        if self._target:
            self._target.reload()
        else:
            try:
                self._target = self.get_target()
            except iSCSITarget.DoesNotExist:
                logger.error('Target with uuid=%s does not exist anymore', self.uuid)
                self.unregister()
        return self._target

    #@property
    #def target(self):
    #    target = None
    #    if self._target:
    #        target = self._target()
    #    if target is not None:
    #        target.reload()
    #    else:
    #        try:
    #            target = self.get_target()
    #        except target.DoesNotExist:
    #            logger.error('Target with uuid=%s does not exist anymore', self.uuid)
    #            self.unregister()
    #        self._target = weakref.ref(target)
    #    return target

    def get_event(self, event):
        event.args.insert(0, self.uuid)
        return event

    def fire_this(self, event):
        return self.fire(self.get_event(event), self.channel)

    @property
    def log_prepend(self):
        target = self.target
        return 'Target %s:' % target

    # When a Peer that we split this target with failsover,
    # become primary for the target.
    # ^ This should already be handled
    #   by resourcemonitor, the exception is when monitor is started while peer
    #   is already dead. In that case, peer_failover will work.
    #@handler('peer_failover', channel='peer')
    #def peer_failover(self, peer):
    #    for dev in self.target.devices:
    #        if dev.remote.uuid != peer.uuid:
    #            continue
    #
    #        # Makes them fight over it. Hmm.
    #        self._check_lun_timer = Timer(10.0 + random.randrange(2, 8), self.get_event(TargetStart())).register(self)
    #        return True

    @handler('resource_role_change', channel='resource')
    def _on_resource_role_change(self, uuid, role):
        res = get_resource(uuid)
        target = self.target
        for group in target.groups:
            for lun in group.luns:
                # TODO what about volume backstores?
                dev = lun.resource
                if dev.uuid == res.uuid:
                    logger.info('%s Member Resource "%s" has become primary.',
                                self.log_prepend, res.name)
                    #if not hasattr(self, '_start_try_timer'):
                    #    self.fire_this(TargetStart())
                    self.fire_this(TargetStart())
                    return

    @handler('resource_secondary_pre', channel='resource')
    def _on_resource_secondary_pre(self, uuid):
        res = get_resource(uuid)
        target = self.target
        for group in target.groups:
            for lun in group.luns:
                # TODO what about volume backstores?
                dev = lun.resource
                if dev.uuid == res.uuid:
                    logger.info('%s Member Resource "%s" wants to become secondary. Trying to '
                                'deconfigure quickly enough so it can.', self.log_prepend, res.name)
                    self.fire_this(TargetStop())
                    #self.log(logger.warning, 'Member Resource "%s" is trying to become secondary while ' +
                    #               'being part of an active target.',
                    #               res.name)

    #@handler('target_start', channel='target')
    def target_start(self, uuid):
        if uuid != self.uuid:
            return
        if hasattr(self, '_start_try_timer'):
            return
        #target = self.target
        #self.log(logger.info, 'Starting..')
        #self.fire_this(TargetStartTry())

        #if hasattr(self, '_start_try_timer'):
        #    self._start_try_timer.unregister()
        #    delattr(self, '_start_try_timer')

        retry_in = 8.0 + random.randrange(2, 8)
        logger.warning('%s Trying to start in %ds.', self.log_prepend, retry_in)
        self._start_try_timer = Timer(retry_in, self.get_event(TargetStartTry()), self.channel).register(self)

    def target_start_try(self, uuid, attempt=0):
        if uuid != self.uuid:
            return
        target = self.target

        def remove_start_try_timer():
            if hasattr(self, '_start_try_timer'):
                self._start_try_timer.unregister()
                delattr(self, '_start_try_timer')

        luns = list(target.get_all_luns())
        missing_luns = list(target.get_all_unavailable_luns())

        #missing_devices = []
        #
        #for dev in target.devices:
        #    # Reload device from database as it may of changed since we last
        #    # retrieved it.
        #    dev.reload()
        #    if not dev.role == 'Primary':
        #        missing_devices.append(dev.name)
        #    if dev.remote_role == 'Primary':
        #        logger.error('%s Cannot start because remote is primary on Resource "%s".',
        #                     self.log_prepend, dev.name)
        #
        #        remove_start_try_timer()
        #        return

        if missing_luns:
            logger.error('%s Cannot start because some luns are not available: "%s".',
                         self.log_prepend, missing_luns)

            # TODO Need to do a random choice between both when they are both
            # destined to be king.
            if attempt > 1 and len(missing_luns) < len(luns):
                if random.randrange(0, 9999) < 5000:
                    for lun in luns:
                        if lun in missing_luns:
                            continue
                        if lun.resource.role == 'Primary':
                            self.fire(ResourceSecondary(lun.resource.uuid), 'resource')
            elif attempt > 5:
                logger.error('%s Giving up on starting for now as some luns are unavailable and attempt=%d',
                             self.log_prepend, attempt)
                remove_start_try_timer()

            remove_start_try_timer()
            retry_in = 8.0 + random.randrange(2, 8)
            logger.warning('%s Retrying to start in %ds.', self.log_prepend, retry_in)
            self._start_try_timer = Timer(retry_in, self.get_event(TargetStartTry(attempt=attempt + 1)), self.channel).register(self)
            return

        logger.info('%s Can now start!', self.log_prepend)
        try:
            target.start()
            logger.info('%s started.', self.log_prepend)
        except Exception, e:
            logger.error('%s Could not start: %s', self.log_prepend, e.message)
            remove_start_try_timer()
            return False
        remove_start_try_timer()
        self.fire_this(TargetStarted())
        return True

    #def target_started(self, event):
    #    logger.debug('%s Got event=%s', self.log_prepend, event)
    #    logger.debug('dir=%s', dir(event))
    #    logger.debug('dict=%s', event.__dict__)

    def target_stop(self, uuid):
        if uuid != self.uuid:
            return
        target = self.target

        logger.info('%s Can now stop!', self.log_prepend)
        target.stop()
        self.fire_this(TargetStopped())
