
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
    def _on_resource_role_change(self, uuid, role):
        res = get_resource(uuid)
        target = self.target
        for lun in target.get_all_luns():
            # TODO what about volume backstores?
            dev = lun.resource
            if dev.uuid == res.uuid:
                logger.info('%s Member Resource "%s" has become primary.',
                            self.log_prepend, res.name)
                if not hasattr(self, '_start_try_timer'):
                    self.fire_this(TargetStart())
                return

    @handler('resource_remote_role_change', channel='resource')
    def _on_resource_remote_role_change(self, uuid, role):
        if not hasattr(self, '_start_try_timer'):
            return
        res = get_resource(uuid)
        target = self.target
        for lun in target.get_all_luns():
            # TODO what about volume backstores?
            dev = lun.resource
            if dev.uuid == res.uuid:
                logger.error('%s Member Resource "%s" has become primary on remote host.',
                             self.log_prepend, res.name)
                if hasattr(self, '_start_try_timer'):
                    self._remove_start_try_timer()
                #self.fire_this(TargetStop())
                return

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

    def target_check_luns(self):
        target = self.target
        if target.enabled:
            return
        missing_luns = list(target.get_all_unavailable_luns())
        if missing_luns:
            logger.error('%s has unavailable luns: %s', self.log_prepend, missing_luns)
            return
        else:
            logger.error('%s all luns are available', self.log_prepend)
            self.fire_this(TargetStart())

    #@handler('target_start', channel='target')
    def target_start(self, uuid):
        if uuid != self.uuid:
            return
        if hasattr(self, '_start_try_timer'):
            return
        self._add_start_try_timer()

    def _remove_start_try_timer(self):
        if hasattr(self, '_start_try_timer'):
            self._start_try_timer.unregister()
            delattr(self, '_start_try_timer')

    def _add_start_try_timer(self, attempt=None):
        if hasattr(self, '_start_try_timer'):
            return
        try_in = 8.0 + random.randrange(2, 8)
        if attempt is None:
            logger.warning('%s Trying to start in %ds.', self.log_prepend, try_in)
            attempt = 0
        else:
            logger.warning('%s Retrying to start in %ds.', self.log_prepend, try_in)
            attempt += 1
        self._start_try_timer = Timer(try_in, self.get_event(TargetStartTry(attempt=attempt)), self.channel).register(self)

    def target_start_try(self, uuid, attempt=0):
        if uuid != self.uuid:
            return
        target = self.target

        luns = list(target.get_all_luns())
        missing_luns = list(target.get_all_unavailable_luns())

        def demote_all_luns_to_secondary(luns):
            for lun in luns:
                if lun.resource.role == 'Primary':
                    self.fire(ResourceSecondary(lun.resource.uuid), 'resource')

        if missing_luns:
            logger.error('%s Cannot start because some luns are not available: "%s".',
                         self.log_prepend, missing_luns)

            # TODO Need to do a random choice between both when they are both
            # destined to be king.
            if attempt > 5 or (attempt > 1 and random.randrange(0, 9999) < 5000):
                if attempt > 5:
                    logger.error('%s Giving up on starting for now as some luns are unavailable and attempt=%d',
                                 self.log_prepend, attempt)
                else:
                    # Try shuffling it up a bit by demoting all of our resources
                    # we do have (which isn't all) to secondary
                    logger.error('%s Switching things up as some luns are unavailable and attempt=%d',
                                 self.log_prepend, attempt)
                demote_all_luns_to_secondary(luns)
                self._remove_start_try_timer()
                return
            else:
                self._remove_start_try_timer()
                self._add_start_try_timer(attempt=attempt)
            return

        logger.info('%s Can now start!', self.log_prepend)
        #try:
        if True:
            target.start()
            logger.info('%s started.', self.log_prepend)
            self._remove_start_try_timer()
            self.fire_this(TargetStarted())
            return True
        #except Exception, e:
        #    logger.error('%s Could not start: %s', self.log_prepend, e.message)
        #    raise e
        #    return False

    #def target_started(self, event):
    #    logger.debug('%s Got event=%s', self.log_prepend, event)
    #    logger.debug('dir=%s', dir(event))
    #    logger.debug('dict=%s', event.__dict__)

    def target_stop(self, uuid):
        if uuid != self.uuid:
            return
        if hasattr(self, '_start_try_timer'):
            self._remove_start_try_timer()

        target = self.target

        logger.info('%s Can now stop!', self.log_prepend)
        target.stop()
        self.fire_this(TargetStopped())
