
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer
from solarsan.ha.models import FloatingIP
import weakref
#from .target import get_target


"""
Failover IP Manager
"""


class FloatingIpsCheck(Event):
    """Check for new FloatingIPs"""


class FloatingIPManager(Component):
    channel = 'floating_ip'

    def __init__(self):
        super(FloatingIPManager, self).__init__()
        self.monitors = {}

        #""" TODO Timer to scan for dual active floating IPs every so often """
        #Timer(30.0, DualFloatingCheck(), self.channel, persist=True).register(self)

    #def started(self, component):
    #    self.fire(FloatingIpsCheck())

    def managers_check(self):
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
        #self.monitors[fip.uuid] = FloatingIPMonitor(fip.uuid, channel='floating_ip-%s' % fip.uuid).register(self)
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
    channel = 'floating_ip'
    uuid = None

    def __init__(self, uuid, channel=channel):
        self.uuid = uuid
        super(FloatingIPMonitor, self).__init__(channel=channel)

        fip = self.fip
        logger.info('Monitoring Floating IP "%s".', fip)

        if fip.is_active:
            ## May want to just disable on startup..
            #logger.warn('Floating IP "%s" is currently active upon startup. Deactivating to verify.', fip)
            #fip.ifdown()
            ##if ip.is_peer_active():
            ##    logger.error('Floating IP "%s" appears to be up in both locations?! Deactivating..', fip)
            ##    """ TODO See which host has the assoc target running too. If one is running it, they win. """
            ##    fip.ifdown()
            logger.warn('Floating IP "%s" is currently active upon startup.', fip)

    def get_fip(self):
        return get_fip(self.uuid)

    _fip = None

    #@property
    #def fip(self):
    #    if self._fip:
    #        self._fip.reload()
    #    else:
    #        try:
    #            self._fip = self.get_fip()
    #        except FloatingIP.DoesNotExist:
    #            logger.error('FloatingIP with uuid=%s does not exist anymore', self.uuid)
    #            self.unregister()
    #    return self._fip

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
        return self.fire(self.get_event(event), self.channel)

    """
    Events
    """

    def floating_ip_start(self, uuid):
        if uuid != self.uuid:
            return
        fip = self.fip
        fip.ifup()

    def floating_ip_stop(self, uuid):
        if uuid != self.uuid:
            return
        fip = self.fip
        fip.ifdown()

    """
    Peer Events
    """
    '''
    # peer_offline hits faster, but we want the IP comeup to be the very last
    # thing to be done throughout a failover.
    @handler('peer_failover', channel='*')
    def _on_peer_failover(self, peer):
        if peer.uuid != self.ip.peer.uuid:
            return
        logger.error('Failing over floating IP "%s" for offline Peer "%s".',
                     self.ip.iface_name, self.ip.peer.hostname)
        self.fire(FloatingIpStart(self.ip))
    '''

    """
    Target event hooks.
    These aren't needed anymore due to signals.
    """
    '''
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
    '''
