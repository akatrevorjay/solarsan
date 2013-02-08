
from solarsan.core import logger
from solarsan.exceptions import TimeoutError
from circuits import Component, Event, Timer, handler
from cluster.models import Peer
import signal

"""
Peer Manager
"""


class PeerHeartbeat(Event):
    """Remote PeerHeartbeat"""
    #complete = True


class PeerHeartbeatTimeout(Event):
    """Remote PeerHeartbeat Timeout"""
    #complete = True


class PeerPoolHealthCheck(Event):
    """Checks Pool Health"""


class PeerManager(Component):
    heartbeat_every = 5.0
    pool_health_every = 10.0

    def __init__(self):
        super(PeerManager, self).__init__()
        self.peers = {}
        self.monitors = {}

        for peer in Peer.objects.all():
            self.add_peer(peer)
        Timer(self.heartbeat_every, PeerHeartbeat(), persist=True).register(self)
        Timer(self.pool_health_every, PeerPoolHealthCheck(), persist=True).register(self)

    @handler('peer_discovered', channel='*')
    def _on_peer_discovered(self, peer, created=None):
        self.add_peer(peer)

    def add_peer(self, peer):
        if peer.uuid in self.peers:
            return
        logger.info("Monitoring Peer '%s'.", peer.hostname)
        self.peers[peer.uuid] = peer
        self.monitors[peer.uuid] = PeerMonitor(peer).register(self)


"""
Peer Monitor
"""


class PeerOnline(Event):
    """Peer Online Event"""


class PeerOffline(Event):
    """Peer Offline Event"""


class PeerFailover(Event):
    """Peer Failover Event"""


class PeerPoolNotHealthy(Event):
    """Pool is Not Healthy"""


class PeerStillOffline(Event):
    """Peer is *still* offline"""


class PeerMonitor(Component):
    heartbeat_timeout_after = 2

    def __init__(self, peer):
        super(PeerMonitor, self).__init__()
        self.peer = peer
        self._heartbeat_timeout_count = None

        if self.peer.is_offline:
            logger.warning('Peer "%s" is already marked as offline. Marking online to ensure this is ' +
                           'still true.', self.peer.hostname)
            self.mark_online(startup=True)

    @handler('peer_heartbeat', channel='*')
    def _on_peer_heartbeat(self):
        # This is done so the first try is always attempted, even if the Peer
        # is offline.
        if not self._heartbeat_timeout_count:
            self._heartbeat_timeout_count = 0
        elif self.peer.is_offline:
            #logger.debug('PeerHeartbeat is not even being attempted as Peer "%s" is marked offline.', self.peer.hostname)
            return

        ret = self.ping()
        if ret:
            self._heartbeat_timeout_count = 0
        else:
            self._heartbeat_timeout_count += 1
            if self._heartbeat_timeout_count >= self.heartbeat_timeout_after:
                self.fire(PeerOffline(self.peer))
        return ret

    def ping(self):
        try:
            storage = self.peer.get_service('storage', default=None)
            storage.ping()
            #logger.debug('PeerHeartbeat back from Peer "%s"', self.peer.hostname)
            return True
        except:
            logger.error('Did not receive heartbeat for Peer "%s"', self.peer.hostname)
            return False

    @handler('peer_online', channel='*')
    def _on_peer_online(self, peer):
        if peer.uuid != self.peer.uuid:
            return
        if self.peer.is_offline:
            self.mark_online()

    @handler('peer_offline', channel='*')
    def _on_peer_offline(self, peer):
        if peer.uuid != self.peer.uuid:
            return
        if not self.peer.is_offline:
            self.mark_offline()

    def mark_online(self, startup=False):
        if not startup:
            logger.warning('Peer "%s" is ONLINE!', self.peer.hostname)
        self.peer.is_offline = False
        self.peer.save()
        if hasattr(self, '_offline_timer'):
            self._offline_timer.unregister()
            delattr(self, '_offline_timer')
        self._heartbeat_timeout_count = 0

    def mark_offline(self):
        logger.error('Peer "%s" is OFFLINE!', self.peer.hostname)
        self.peer.is_offline = True
        self.peer.save()
        self._offline_timer = Timer(60.0, PeerStillOffline(self.peer), persist=True).register(self)
        self.fire(PeerFailover(self.peer))

    # nagnagnagnagnagnag
    @handler('peer_still_offline', channel='*')
    def _on_peer_still_offline(self, peer):
        if peer.uuid != self.peer.uuid:
            return
        logger.warning("Peer '%s' is STILL offline!", peer.hostname)

    @handler('peer_discovered', channel='*')
    def _on_peer_discovered(self, peer, created=False):
        if peer.uuid != self.peer.uuid:
            return
        if self.peer.is_offline:
            self.fire(PeerOnline(self.peer))

    @handler('peer_pool_health_check', channel='*')
    def _on_peer_pool_health_check(self):
        if self.peer.is_offline:
            return

        def timeout(signum, frame):
            raise TimeoutError('Pool check on Peer "%s" timed out.' % self.peer.hostname)

        # Set the signal handler and a 5-second alarm
        signal.signal(signal.SIGALRM, timeout)
        signal.alarm(5)

        try:
            try:
                storage = self.peer.get_service('storage', default=None)
                Pool = storage.root.pool()
            except AttributeError:
                logger.error('Could not connect to Peer "%s"; is it offline and we don\'t know it yet?',
                             self.peer.hostname)
                return
            pools = Pool.list()
            #logger.debug('Checking health of Pools "%s" on Peer "%s"', pools, self.peer.hostname)
            ret = True
            for pool in pools:
                if not pool.is_healthy():
                    self.fire(PeerPoolNotHealthy(self.peer, pool.name))
                    ret = False
        except TimeoutError, e:
            logger.error('Could not check pools on Peer "%s": %s',
                         self.peer.hostname, e.message)

        signal.alarm(0)          # Disable the alarm
        return ret

    @handler('peer_pool_not_healthy', channel='*')
    def _on_peer_pool_not_healthy(self, peer, pool):
        if peer.uuid != self.peer.uuid:
            return
        logger.error('Pool "%s" on Peer "%s" is NOT healthy!', pool, self.peer.hostname)

        # TODO If Pool has remote disks, and those are the only ones that are
        # missing, try a 'zpool clear'
