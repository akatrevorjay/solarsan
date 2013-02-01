#!/usr/bin/env python

from solarsan.core import logger
#from solarsan import conf
from storage.drbd import DrbdResource
from circuits import Component, Event, Debugger, Timer
from datetime import datetime
from cluster.models import Peer
from target.models import iSCSITarget
import target.scst
import rpyc


"""
Events
"""


class DiscoverPeers(Event):
    """Discover Event"""


class ProbePeer(Event):
    """Peer Probe Event"""


class PeerOnline(Event):
    """Peer Online Event"""


class PeerOffline(Event):
    """Peer Offline Event"""


class ReconnectPeer(Event):
    """Peer reconnection attempt Event"""


class PeerHeartbeat(Event):
    """Remote PeerHeartbeat"""


class PromoteToPrimary(Event):
    """Promote Resource to Primary"""


"""
Mother Discovery


For each drbd resource:
    Every so often, check remote status.
    If status == False:
        If local.is_primary:
            1. Log event
        else:
            1. Get a list of all resources shared with remote
            2. Become primary on all of them
            3. Write new target config
            4. Reload target config
            5. Enable HA IPs

"""


class MotherDiscovery(Component):
    discover_every = 60.0
    heartbeat_every = 5.0

    def __init__(self):
        super(MotherDiscovery, self).__init__()

        self.peers = []

        # Save discover peers event
        self.e_discover = DiscoverPeers()
        self.e_discover.complete = True

        # Save heartbeat event
        self.e_heartbeat = PeerHeartbeat()
        self.e_heartbeat.complete = True

        self.monitor_setup()

    def started(self, *args):
        self.fire(self.e_discover)

    """
    Monitor
    """

    def monitor_setup(self):
        # Setup
        self.remotes = {}
        for res in DrbdResource.objects.all():
            if not res.remote.hostname in self.remotes:
                self.remotes[res.remote.hostname] = []
            self.remotes[res.remote.hostname].append(res)

        self.peer_monitors = {}
        for hostname, resources in self.remotes.iteritems():
            self.peer_monitors[hostname] = PeerMonitor(hostname, resources).register(self)

    def peer_heartbeat_complete(self, *results):
        self.heartbeat_timer = Timer(self.heartbeat_every, self.e_heartbeat, *self.peer_monitors).register(self)

    """
    Discovery
    """

    def discover_peers(self):
        logger.info("Discovering nearby peers..")
        try:
            for host, port in rpyc.discover('storage'):
                self.fire(ProbePeer(host))
        except Exception:
            pass

    def discover_peers_complete(self, *args):
        #logger.info("Discovery complete.")
        # Do this again every so often
        self.fire(self.e_heartbeat)
        self.discover_timer = Timer(self.discover_every, self.e_discover).register(self)

    def probe_peer(self, host):
        """Probes a discovered node for info"""

        hostname = None
        ifaces = None
        addrs = None
        cluster_iface = None

        try:
            c = rpyc.connect_by_service('storage', host=host)

            hostname = c.root.peer_hostname()
            cluster_iface = c.root.peer_get_cluster_iface()

            ifaces = c.root.peer_list_addrs()
            addrs = dict([(y['addr'], y['netmask']) for x in ifaces.values() for y in x])

            if None in [hostname, ifaces, addrs, cluster_iface]:
                raise Exception("Peer discovery probe has invalid data.")

            logger.info("Peer discovery (host='%s'): Hostname is '%s'.", host, hostname)
        except Exception, e:
            logger.error("Peer discovery (host='%s') failed: %s", host, e)

        # TODO Each node should prolly get a UUID, glusterfs already assigns one, but maybe we
        # should do it a layer above.

        peer, created = Peer.objects.get_or_create(hostname=hostname)

        peer.addrs = list(addrs.keys())
        peer.netmasks = list(addrs.values())
        peer.ifaces = list(ifaces.keys())

        peer.cluster_addr = cluster_iface['ipaddr']
        peer.cluster_iface = cluster_iface['name']

        peer.last_seen = datetime.utcnow()
        send_peer_online = bool(created or peer.is_offline)
        peer.is_offline = None

        peer.save()

        if send_peer_online:
            self.fire(PeerOnline(peer))

        return True


"""
Peer Monitor
"""


class PeerMonitor(Component):
    heartbeat_timeout_window = 15.0
    offline_reconnect_interval = 30.0

    def __init__(self, peer_hostname, resources):
        super(PeerMonitor, self).__init__()
        self.peer = Peer.objects.get(hostname=peer_hostname)

        self.resources = resources
        self.res_mons = []
        for res in resources:
            self.res_mons.append(ResourceMonitor(res).register(self))

    def started(self, *args):
        self.peer_online()

    def peer_heartbeat(self):
        if self.peer.is_offline:
            logger.debug('PeerHeartbeat is not even being attempted as Peer "%s" is marked offline.', self.peer.hostname)
            return

        try:
            ret = self.ping()

            if hasattr(self, 'timeout_timer'):
                # Timer is used to know when our timeout window has been up.
                # Since this went through fine, reset the timer to 0.
                self.timeout_timer.reset()
        except:
            ret = None
        return ret

    def ping(self):
        try:
            self.peer.storage.ping()
            logger.debug('PeerHeartbeat back from Peer "%s"', self.peer.hostname)
            return True
        except:
            logger.error('Did not receive heartbeat for Peer "%s"', self.peer.hostname)
            raise

    def peer_reconnect_attempt(self):
        ret = self.ping()
        self.reconnection_timer = Timer(self.offline_reconnect_interval, ReconnectPeer()).register(self)
        self.fire(PeerOnline(self.peer))
        return ret

    def peer_offline(self):
        if self.peer.is_offline:
            return

        logger.error('Peer "%s" is OFFLINE!', self.peer.hostname)
        self.peer.is_offline = True
        self.peer.save()

        # Remove timeout timer
        if hasattr(self, 'timeout_timer'):
            self.timeout_timer.unregister()
            delattr(self, 'timeout_timer')

        # Add reconnect timer
        self.reconnect_timer = Timer(self.offline_reconnect_interval, ReconnectPeer()).register(self)
        #self.fire(ReconnectPeer())

    def peer_online(self):
        if not self.peer.is_offline:
            return

        logger.error('Peer "%s" is back ONLINE!', self.peer.hostname)
        self.peer.is_offline = False
        self.peer.save()

        # Remove reconnect timer
        if hasattr(self, 'reconnect_timer'):
            self.reconnect_timer.unregister()
            delattr(self, 'reconnect_timer')

        # Add timeout timer
        self.timeout_timer = Timer(self.heartbeat_timeout_window, PeerOffline()).register(self)

    #def peer_offline(self):
    #    logger.info('Promoting self to Primary for all Resources with dead Peer "%s".')
    #    e = PromoteToPrimary()
    #    e.complete = True
    #    self.fire(e)

    #def peer_heartbeat_complete(self, *results):
    #    logger.debug('Peer "%s" is heartbeat_complete', self.peer.hostname)

    def promote_to_primary(self):
        logger.info('Taking over as Primary for all Resources with Peer "%s"', self.remote.hostname)
        for res in self.resources:
            sl = res.local.service
            if not sl.is_primary:
                sl.primary()
        logger.info('Now Primary for all Resources with Peer "%s"', self.remote.hostname)


"""
Resource Monitor
"""


class ResourcePrimary(Event):
    """Takeover as Primary"""


class ResourceSecondary(Event):
    """Takeover as Primary"""


class ResourceActive(Event):
    """Mark this box as active. This is done after Primary."""


class ResourcePassive(Event):
    """Mark this box as passive. This is done before Secondary."""


class ResourceMonitor(Component):
    def __init__(self, res):
        super(ResourceMonitor, self).__init__()
        self.res = res
        self._status = None

    def started(self, *args):
        pass

    @property
    def service(self):
        return self.res.local.service

    def peer_heartbeat(self):
        status = self.service.status()

        # If we have two secondary nodes become primary
        if status['role'] == 'Secondary' and status['remote_role'] == 'Secondary':
            logger.info('Taking over as Primary for Resource "%s" because someone has to ' +
                        '(dual secondaries).', % self.res.name)
            self.primary()

        self._status = status.copy()

    def get_primary(self):
        if self._status['role'] == 'Primary':
            return self.res.local
        elif self._status['remote_role'] == 'Primary':
            return self.res.remote

    def write_config(self, adjust=True):
        self.service.write_config()
        if adjust:
            self.service.adjust()
        return True

    def primary(self):
        self.service.primary()

    def secondary(self):
        self.service.secondary()

    def target_scst_write_config_and_reload(self):
        self.service.scst_write_config()
        self.service.scst_clear_config()
        self.service.scst_reload_config()


def run_ha():
    try:
        #(MotherDiscovery() + Monitor()).run()
        (MotherDiscovery() + Debugger()).run()
    except (SystemExit, KeyboardInterrupt):
        raise


"""
Main
"""


def main():
    run_ha()


if __name__ == '__main__':
    main()
