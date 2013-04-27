
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.exceptions import SolarSanError
from solarsan.cluster.models import Peer
from circuits import Component, Timer, Event
from datetime import datetime
import rpyc


"""
Discovery
"""


class DiscoverPeers(Event):
    """Discover Event"""
    #complete = True


class ProbePeer(Event):
    """Peer Probe Event"""


class PeerDiscovered(Event):
    """Peer Discovered Event"""


class DiscoveryError(SolarSanError):
    pass


class Discovery(Component):
    channel = 'discovery'

    discover_every = 60.0

    def __init__(self):
        super(Discovery, self).__init__()

        Timer(self.discover_every, DiscoverPeers(), self.channel, persist=True).register(self)

    def managers_check(self):
        self.fire(DiscoverPeers())

    """
    Discovery
    """

    def discover_peers(self):
        logger.info("Discovering nearby peers..")
        try:
            for host, port in rpyc.discover('storage'):
                self.fire(ProbePeer(host))
        except Exception as e:
            logger.error('Got error while discovering nearby peers: %s', e)

    def probe_peer(self, host):
        """Probes a discovered node for info"""

        hostname = None
        ifaces = None
        addrs = None
        cluster_iface = None

        try:
            c = rpyc.connect_by_service('storage', host=host)

            hostname = str(c.root.peer_hostname())
            uuid = str(c.root.peer_uuid())
            cluster_iface = dict(c.root.peer_get_cluster_iface())

            ifaces = dict(c.root.peer_list_addrs())
            addrs = dict([(y['addr'], y['netmask']) for x in ifaces.values() for y in x])

            if None in [hostname, uuid, ifaces, addrs, cluster_iface]:
                raise DiscoveryError("Failed to probe discovered peer host=%s: Probe has invalid data.", host)

            logger.debug("Discovered peer host=%s; hostname=%s.", host, hostname)
        except Exception, e:
            logger.error("Failed to probe discovered peer host=%s: %s", host, e)
            return
        finally:
            try:
                c.close()
                c = None
            except:
                pass

        peer, created = Peer.objects.get_or_create(uuid=uuid, defaults={'hostname': hostname})

        peer.hostname = hostname
        peer.addrs = list(addrs.keys())
        peer.netmasks = list(addrs.values())
        peer.ifaces = list(ifaces.keys())

        peer.cluster_addr = cluster_iface['ipaddr']
        peer.cluster_iface = cluster_iface['name']

        peer.last_seen = datetime.utcnow()

        peer.save()

        self.fire(PeerDiscovered(peer.uuid, created=created))
        return True
