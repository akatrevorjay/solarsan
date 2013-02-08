
from solarsan.core import logger
from circuits import Component, Event, handler
from solarsan.ha.models import ActivePassiveIP


"""
Failover IP Manager
"""


class FloatingIPManager(Component):
    def __init__(self):
        super(FloatingIPManager, self).__init__()
        self.ips = {}
        self.monitors = {}

        for ip in ActivePassiveIP.objects.all():
            self.ips[ip.name] = ip
            self.monitors[ip.name] = FloatingIPMonitor(ip).register(self)


"""
Failover IP Monitor
"""


class ActiveIP(Event):
    """Start FloatingIP"""


class PassiveIP(Event):
    """Stop FloatingIP"""


class FloatingIPMonitor(Component):
    def __init__(self, ip):
        super(FloatingIPMonitor, self).__init__()
        self.ip = ip

        logger.info('Monitoring Floating IP "%s".', self.ip.iface_name)

        if self.ip.is_active:
            logger.warn('Floating IP "%s" is currently active upon startup.', self.ip.iface_name)
            # May want to just disable on startup..
            #logger.warn('Floating IP "%s" is currently active upon startup. Deactivating..', self.ip.iface_name)
            self.ip.ifdown()
            #if ip.is_peer_active():
            #    logger.error('Floating IP "%s" appears to be up in both locations?! Deactivating..', self.ip.iface_name)
            #
            #    """ TODO See which host has the assoc target running too. If one is running it, they win. """
            #
            #    self.ip.ifdown()

            """ TODO Timer to scan for dual active floating IPs every so often """
            #Timer(30.0, DualFloatingCheck(), persist=True).register(self)

    ## peer_offline hits faster, but we want the IP comeup to be the very last
    ## thing to be done throughout a failover.
    #@handler('peer_failover', channel='*')
    #def _on_peer_failover(self, peer):
    #    if peer.uuid != self.ip.peer.uuid:
    #        return
    #    logger.error('Failing over floating IP "%s" for offline Peer "%s".',
    #                 self.ip.iface_name, self.ip.peer.hostname)
    #    self.fire(ActiveIP(self.ip))

    @handler('target_started', channel='*')
    def _on_target_started(self, target):
        """When a Target assoc with this floating IP has been started, start her up"""
        if target.floating_ip.pk != self.ip.pk:
            return
        self.fire(ActiveIP(self.ip))

    @handler('target_stopping', channel='*')
    def _on_target_stopping(self, target):
        """When a Target assoc with this floating IP has been stopped, start her up"""
        if target.floating_ip.pk != self.ip.pk:
            return
        self.fire(PassiveIP(self.ip))

    @handler('active_ip', channel='*')
    def _on_active_ip(self, ip):
        if ip.pk != self.ip.pk:
            return
        self.ifup()

    @handler('passive_ip', channel='*')
    def _on_passive_ip(self, ip):
        if ip.pk != self.ip.pk:
            return
        self.ifdown()

    def ifup(self):
        if self.ip.is_active:
            return
        logger.info('Floating IP "%s" is being brought up.', self.ip.iface_name)
        self.ip.ifup()

    def ifdown(self):
        if not self.ip.is_active:
            return
        logger.info('Floating IP "%s" is being brought down.', self.ip.iface_name)
        self.ip.ifdown()

