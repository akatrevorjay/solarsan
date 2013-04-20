
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
        self.fire(TargetCheckLuns())

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


class TargetCheckLuns(Event):
    pass


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
            self.stop()
            self.detach_all_luns()
        else:
            self.detach_all_luns()

    def get_target(self):
        return get_target(self.uuid)

    _target = None

    #@property
    #def target(self):
    #    if self._target:
    #        self._target.reload()
    #    else:
    #        try:
    #            self._target = self.get_target()
    #        except iSCSITarget.DoesNotExist:
    #            logger.error('Target with uuid=%s does not exist anymore', self.uuid)
    #            self.unregister()
    #    return self._target

    @property
    def target(self):
        target = None
        if self._target:
            target = self._target()
        if target is not None:
            target.reload()
        else:
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
        return self.fire(self.get_event(event))

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
    def _on_resource_role_change(self, uuid, role, old=None):
        res = get_resource(uuid)
        target = self.target
        for lun in target.get_all_luns():
            # TODO what about volume backstores?
            dev = lun.resource
            if dev.uuid == res.uuid:
                if old == 'Secondary' and role == 'Primary':
                    logger.info('%s Member Resource "%s" has become primary.',
                                self.log_prepend, res.name)
                    if not target.enabled and not self._start_timer:
                        self.fire_this(TargetStart())
                        yield None
                #elif old == 'Primary' and role == 'Secondary':
                #    logger.info('%s Member Resource "%s" has become secondary.',
                #                self.log_prepend, res.name)
                #    #if target.enabled:
                #    #    self.fire_this(TargetStop())
                #    #else:
                #    #if True:
                #    #    self.detach_all_luns()

    #@handler('resource_remote_role_change', channel='resource')
    #def _on_resource_remote_role_change(self, uuid, role, old=None):
    #    res = get_resource(uuid)
    #    target = self.target
    #    for lun in target.get_all_luns():
    #        # TODO what about volume backstores?
    #        dev = lun.resource
    #        if dev.uuid == res.uuid:
    #            if old == 'Secondary' and role == 'Primary':
    #                logger.error('%s Member Resource "%s" has become primary on remote host.',
    #                             self.log_prepend, res.name)
    #                self.stop_timer()
    #
    #                #if target.enabled:
    #                #    self.fire_this(TargetStop())
    #                #else:
    #                #if True:
    #                #    self.detach_all_luns()
    #            elif old == 'Primary' and role == 'Secondary':
    #                if not target.enabled and not self._start_timer:
    #                    self.fire_this(TargetStart())

    @handler('resource_secondary_pre', channel='resource')
    def _on_resource_secondary_pre(self, uuid):
        res = get_resource(uuid)
        target = self.target
        for lun in target.get_all_luns():
            # TODO what about volume backstores?
            dev = lun.resource
            if dev.uuid == res.uuid:
                logger.info('%s Member Resource "%s" wants to become secondary. Trying to '
                            'deconfigure quickly enough so it can.', self.log_prepend, res.name)
                self.fire_this(TargetStop())
                #self.log(logger.warning, 'Member Resource "%s" is trying to become secondary while ' +
                #               'being part of an active target.',
                #               res.name)

    def detach_all_luns(self):
        target = self.target
        logging.info('%s Shuffling it up; detaching attached luns.', self.log_prepend)
        for lun in list(target.get_all_luns()):
            if lun.is_available:
                lun.detach()

    def target_check_luns(self, fire=True):
        target = self.target
        missing_luns = list(target.get_all_unavailable_luns())
        ret = None
        if missing_luns and target.enabled:

            logger.error('%s has unavailable luns: %s', self.log_prepend, missing_luns)
            #logger.error('%s Cannot start because some luns are not available: "%s".',
            #             self.log_prepend, missing_luns)
            ret = False
            if fire:
                self.fire_this(TargetStop())
        elif not missing_luns and not target.enabled:
            logger.error('%s all luns are available', self.log_prepend)
            ret = True
            if fire:
                self.fire_this(TargetStart())

        if fire:
            return ret
        else:
            return (ret, missing_luns)

    start_attempts = 5

    def target_start(self, uuid, attempt=0):
        if uuid != self.uuid:
            return

        target = self.target
        if target.enabled:
            #logger.info('%s is already started.', self.log_prepend)
            #self.stop_timer()
            return

        if attempt > 0:
            ret, missing_luns = self.target_check_luns(fire=False)
            if ret:
                logger.info('%s Can now start!', self.log_prepend)
                try:
                    target.start()
                    logger.info('%s started.', self.log_prepend)
                    self.stop_timer()
                    self.fire_this(TargetStarted())
                    return True
                except Exception, e:
                    #logger.error('%s Could not start: %s', self.log_prepend, e.message)
                    logger.exception('%s Could not start: %s', self.log_prepend, e.message)
                    #raise e
                    #return False

        if attempt < self.start_attempts:
            try_in = 5.0 + random.randrange(1, 5)
            self._start_timer = Timer(try_in, self.get_event(TargetStart(attempt=attempt + 1)), self.channel).register(self)

    def target_started(self, uuid):
        if uuid != self.uuid:
            return
        self.stop_timer()

    #def start(self, attempt=0):
    #    if attempt == 0 and self._start_timer:
    #        return
    #    retry = attempt <= self.start_attempts

    #    self.stop_timer()
    #    if retry:
    #        if attempt > 2 and random.randrange(0, 100) > 50:
    #            self.detach_all_luns()

    #        if attempt == 0:
    #            logger.warning('%s Trying to start in %ss.', self.log_prepend, try_in)
    #        else:
    #            logger.warning('%s Retrying to start in %ds. (attempt=%d)', self.log_prepend, try_in, attempt)

    #        attempt += 1
    #        self._start_timer = Timer(try_in, self.get_event(TargetStart(attempt=attempt)), self.channel).register(self)
    #        return
    #    else:
    #        logger.error('%s Giving up on starting for now as some luns are unavailable and attempt=%d',
    #                     self.log_prepend, attempt)
    #        self.detach_all_luns()
    #        return False

    def target_stop(self, uuid):
        if uuid != self.uuid:
            return
        return self.stop()

    _start_timer = None

    def stop_timer(self):
        if self._start_timer:
            self._start_timer.unregister()
            self._start_timer = None
            return True

    def stop(self):
        target = self.target
        if target.enabled:
            logger.info('%s Can now stop!', self.log_prepend)
            target.stop()
            self.fire_this(TargetStopped())
