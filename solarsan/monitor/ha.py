
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer
from solarsan.ha.models import FloatingIP
import weakref
from .target import get_target


"""
Failover IP Manager
"""


class FloatingIpsCheck(Event):
    """Check for new FloatingIPs"""


class FloatingIPManager(Component):
    check_every = 300.0

    def __init__(self):
        super(FloatingIPManager, self).__init__()
        self.monitors = {}

        self.floating_ips_check()

        self._check_timer = Timer(self.check_every,
                                  FloatingIpsCheck(),
                                  persist=True,
                                  ).register(self)

    def floating_ips_check(self):
        uuids = []
        for fip in FloatingIP.objects.all():
            self.add_floating_ip(fip)
            uuids.append(fip.uuid)
        for uuid in self.monitors.keys():
            if uuid not in uuids:
                self.monitors[uuid].unregister()
                self.monitors.pop(uuid)

    def add_floating_ip(self, fip):
        if fip.uuid in self.monitors:
            return
        self.monitors[fip.uuid] = FloatingIPMonitor(fip.uuid).register(self)


"""
Failover IP Monitor
"""


class FloatingIpStart(Event):
    """Start FloatingIP"""


class FloatingIpStop(Event):
    """Stop FloatingIP"""


def get_fip(uuid):
    return FloatingIP.objects.get(uuid=uuid)


class FloatingIPMonitor(Component):
    uuid = None

    def __init__(self, uuid):
        self.uuid = uuid
        super(FloatingIPMonitor, self).__init__()

        fip = self.fip
        logger.info('Monitoring Floating IP "%s".', fip.iface_name)

        if fip.is_active:
            #logger.warn('Floating IP "%s" is currently active upon startup.', fip.iface_name)
            # May want to just disable on startup..
            logger.warn('Floating IP "%s" is currently active upon startup. Deactivating..', fip.iface_name)
            fip.ifdown()
            #if ip.is_peer_active():
            #    logger.error('Floating IP "%s" appears to be up in both locations?! Deactivating..', fip.iface_name)
            #    """ TODO See which host has the assoc target running too. If one is running it, they win. """
            #    fip.ifdown()

            #""" TODO Timer to scan for dual active floating IPs every so often """
            #Timer(30.0, DualFloatingCheck(), persist=True).register(self)

    def get_fip(self):
        return get_fip(self.uuid)

    _fip = None

    @property
    def fip(self):
        fip = None
        if self._fip:
            fip = self._fip()
        if fip is None:
            try:
                fip = self.get_fip()
            except fip.DoesNotExist:
                logger.error('FloatingIP with uuid=%s does not exist anymore', self.uuid)
                self.unregister()
            self._fip = weakref.ref(fip)
        return fip

    def get_event(self, event):
        event.args.insert(0, self.uuid)
        return event

    def fire_this(self, event):
        event.args.insert(0, self.uuid)
        return self.fire(event)

    ## peer_offline hits faster, but we want the IP comeup to be the very last
    ## thing to be done throughout a failover.
    #@handler('peer_failover', channel='*')
    #def _on_peer_failover(self, peer):
    #    if peer.uuid != self.ip.peer.uuid:
    #        return
    #    logger.error('Failing over floating IP "%s" for offline Peer "%s".',
    #                 self.ip.iface_name, self.ip.peer.hostname)
    #    self.fire(FloatingIpStart(self.ip))

    def target_started(self, uuid):
        """When a Target assoc with this floating IP has been started, start her up"""
        target = get_target(uuid)
        if target.floating_ip.uuid != self.uuid:
            return
        self.fire_this(FloatingIpStart())

    def target_stopping(self, uuid):
        """When a Target assoc with this floating IP has been stopped, start her up"""
        target = get_target(uuid)
        if target.floating_ip.uuid != self.uuid:
            return
        self.fire_this(FloatingIpStop())

    def floating_ip_start(self, uuid):
        if uuid != self.uuid:
            return
        fip = self.fip
        logger.info('Floating IP "%s" is being brought up.', fip.iface_name)
        fip.ifup()

    def floating_ip_stop(self, uuid):
        if uuid != self.uuid:
            return
        fip = self.fip
        logger.info('Floating IP "%s" is being brought down.', fip.iface_name)
        fip.ifdown()
