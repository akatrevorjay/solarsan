#!/usr/bin/env python

from solarsan.core import logger
#from solarsan import conf
from solarsan.exceptions import FormattedException
from storage.drbd import DrbdResource
from circuits import Component, Event, Debugger, Timer
from circuits.tools import inspect
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
    complete = True


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
    complete = True


class PromoteToPrimary(Event):
    """Promote Resource to Primary"""


class DemoteToSecondary(Event):
    """Demote Resource to Secondary"""


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
    discover_every = 10.0
    heartbeat_every = 5.0

    def __init__(self):
        super(MotherDiscovery, self).__init__()

        self.peers = []

        # Sort resources by remote Peer
        self.remotes = {}
        self.peer_monitors = {}
        self.resources_grouped_by_remote()

        Timer(self.discover_every, DiscoverPeers(), persist=True).register(self)
        Timer(self.heartbeat_every, PeerHeartbeat(), persist=True).register(self)

    def started(self, *args):
        pass

    """
    Monitor
    """

    def resources_grouped_by_remote(self):
        for res in DrbdResource.objects.all():
            if not res.remote.hostname in self.remotes:
                self.remotes[res.remote.hostname] = []
            if res.name in [x.name for x in self.remotes[res.remote.hostname]]:
                continue
            self.remotes[res.remote.hostname].append(res)

        for hostname, resources in self.remotes.iteritems():
            if hostname in self.peer_monitors:
                continue
            self.peer_monitors[hostname] = PeerMonitor(hostname, resources).register(self)

    def peer_heartbeat_complete(self, *results):
        pass

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
        self.resources_grouped_by_remote()

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
            #raise FormattedException("Peer discovery (host='%s') failed: %s", host, e)
            logger.error("Peer discovery (host='%s') failed: %s", host, e)
            return

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
            self.fire(PeerOnline(peer, resources=self.remotes[peer.hostname]))

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
        self.peer_online(self.peer, self.resources)

    def ping(self):
        try:
            self.peer.storage.ping()
            logger.debug('PeerHeartbeat back from Peer "%s"', self.peer.hostname)
            return True
        except:
            logger.error('Did not receive heartbeat for Peer "%s"', self.peer.hostname)
            raise

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

    def reconnect_peer(self, peer=None, resources=None):
        if peer and self.peer.hostname != peer.hostname:
            return

        logger.error('Attempting to reconnect to Peer "%s"', self.peer.hostname)
        try:
            ret = self.ping()
            self.peer_online(self.peer, rsources=self.resources)
            self.fire(PeerOnline(self.peer))
        except:
            if not hasattr(self, 'reconnect_timer'):
                self.reconnect_timer = Timer(self.offline_reconnect_interval, ReconnectPeer(peer, resources=resources)).register(self)
        return ret

    def remove_timers(self, timeout_timer=False, reconnect_timer=False):
        timers = []
        if timeout_timer:
            timers.append('timeout_timer')
        if reconnect_timer:
            timers.append('reconnect_timer')

        for timer_name in timers:
            timer = getattr(self, timer_name, None)
            if timer:
                timer.unregister()
                delattr(self, timer_name)

    def peer_offline(self, peer=None, resources=None):
        if peer and self.peer.hostname != peer.hostname:
            return

        logger.error('Peer "%s" is OFFLINE!', self.peer.hostname)
        self.peer.is_offline = True
        self.peer.save()

        # Remove timeout timer
        self.remove_timers(timeout_timer=True, reconnect_timer=True)

        # Take over the volumes, write out any targets, enable any HA ips, etc
        self.fire(PromoteToPrimary(peer, resources=resources))

        if not hasattr(self, 'reconnect_timer'):
            # Add reconnect timer
            self.reconnect_timer = Timer(self.offline_reconnect_interval,
                                         ReconnectPeer(peer, resources=resources)
                                         ).register(self)

    def peer_online(self, peer=None, resources=None):
        if peer and self.peer.hostname != peer.hostname:
            return

        logger.warning('Peer "%s" is back ONLINE!', self.peer.hostname)
        self.peer.is_offline = False
        self.peer.save()

        # Remove reconnect timer
        self.remove_timers(timeout_timer=True, reconnect_timer=True)

        if not hasattr(self, 'timeout_timer'):
            # Add timeout timer
            self.timeout_timer = Timer(self.heartbeat_timeout_window,
                                       PeerOffline(peer, resources=self.resources)
                                       ).register(self)

    def promote_to_primary(self, peer=None, resources=None):
        logger.info('Taking over as Primary: all Resources with Peer "%s"',
                    self.remote.hostname)

        for res in self.resources:
            sl = res.local.service
            if not sl.is_primary:
                sl.primary()

        # TODO WRITE OUT ANY TARGETS, SCST CONFIG AS APPLIES

        # TODO ENABLE ANY HA IPS

        logger.info('Done taking over as Primary: all all Resources with Peer "%s"',
                    self.remote.hostname)

    def demote_to_secondary(self):
        logger.info('Demoting myself to Secondary: all Resources with Peer "%s"',
                    self.remote.hostname)

        for res in self.resources:
            sl = res.local.service
            if not sl.is_secondary:
                sl.secondary()

        # TODO WRITE OUT ANY TARGETS, SCST CONFIG AS APPLIES

        # TODO ENABLE ANY HA IPS

        logger.info('Done demoting myself to Secondary: all Resources with Peer "%s"',
                    self.remote.hostname)


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
            logger.info('Taking over as Primary for Resource "%s" because someone has to. ' +
                        '(dual secondaries).', self.res.name)
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
