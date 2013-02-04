
from solarsan.core import logger
from cluster.models import Peer
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


class Discovery(Component):
    discover_every = 60.0

    def __init__(self):
        super(Discovery, self).__init__()

    def started(self, *args):
        self.fire(DiscoverPeers())
        Timer(self.discover_every, DiscoverPeers(), persist=True).register(self)

    """
    Discovery
    """

    def discover_peers(self):
        #logger.debug("Discovering nearby peers..")
        try:
            for host, port in rpyc.discover('storage'):
                self.fire(ProbePeer(host))
        except Exception:
            pass

    def probe_peer(self, host):
        """Probes a discovered node for info"""

        hostname = None
        ifaces = None
        addrs = None
        cluster_iface = None

        try:
            c = rpyc.connect_by_service('storage', host=host)

            hostname = c.root.peer_hostname()
            uuid = c.root.peer_uuid()
            cluster_iface = c.root.peer_get_cluster_iface()

            ifaces = c.root.peer_list_addrs()
            addrs = dict([(y['addr'], y['netmask']) for x in ifaces.values() for y in x])

            if None in [hostname, uuid, ifaces, addrs, cluster_iface]:
                raise Exception("Peer discovery probe has invalid data.")

            #logger.info("Peer discovery (host='%s'): Hostname is '%s'.", host, hostname)
        except Exception, e:
            #raise FormattedException("Peer discovery (host='%s') failed: %s", host, e)
            logger.error("Peer discovery (host='%s') failed: %s", host, e)
            return

        peer, created = Peer.objects.get_or_create(uuid=uuid, defaults={'hostname': hostname})

        peer.hostname = hostname
        peer.addrs = list(addrs.keys())
        peer.netmasks = list(addrs.values())
        peer.ifaces = list(ifaces.keys())

        peer.cluster_addr = cluster_iface['ipaddr']
        peer.cluster_iface = cluster_iface['name']

        peer.last_seen = datetime.utcnow()

        peer.save()

        self.fire(PeerDiscovered(peer, created=created))
        return True
