
from solarsan import logging
logger = logging.getLogger(__name__)
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


class Discovery(Component):
    channel = 'discovery'

    discover_every = 60.0

    def __init__(self):
        super(Discovery, self).__init__()

        Timer(self.discover_every, DiscoverPeers(), self.channel, persist=True).register(self)

    def started(self, component):
        self.fire(DiscoverPeers())

    """
    Discovery
    """

    def discover_peers(self):
        logger.debug("Discovering nearby peers..")
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
                raise Exception("Peer discovery probe has invalid data.")

            logger.debug("Peer discovery (host='%s'): Hostname is '%s'.", host, hostname)
        except Exception, e:
            logger.error("Peer discovery (host='%s') failed: %s", host, e)
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
