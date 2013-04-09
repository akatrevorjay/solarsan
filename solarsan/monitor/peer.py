
from solarsan.core import logger
#from solarsan.exceptions import TimeoutError
from circuits import Component, Event, Timer, handler
from solarsan.cluster.models import Peer
#import signal
import weakref


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
        self.monitors = {}

        for peer in Peer.objects.all():
            self.add_peer(peer)

        Timer(self.heartbeat_every, PeerHeartbeat(), persist=True).register(self)
        Timer(self.pool_health_every, PeerPoolHealthCheck(), persist=True).register(self)

    def peer_discovered(self, peer, created=None):
        self.add_peer(peer)
        return True

    def add_peer(self, peer):
        if peer.uuid in self.monitors:
            return
        logger.info("Monitoring Peer '%s'.", peer.hostname)
        self.monitors[peer.uuid] = PeerMonitor(peer.uuid).register(self)


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


def get_peer(uuid):
    return Peer.objects.get(uuid=uuid)


class PeerMonitor(Component):
    heartbeat_timeout_after = 2

    uuid = None

    def __init__(self, uuid):
        self.uuid = uuid
        super(PeerMonitor, self).__init__()
        self._heartbeat_timeout_count = None

        peer = self.peer
        if peer.is_offline:
            logger.warning('Peer "%s" is already marked as offline. Marking online to ensure this is ' +
                           'still true.', peer.hostname)
            self.mark_online(startup=True)

    def get_peer(self):
        return get_peer(self.uuid)

    _peer = None

    @property
    def peer(self):
        peer = None
        if self._peer:
            peer = self._peer()
        if peer is None:
            try:
                peer = self.get_peer()
            except DrbdResource.DoesNotExist:
                logger.error('Peer with uuid=%s does not exist anymore', self.uuid)
                self.unregister()
            self._peer = weakref.ref(peer)
        return peer

    def get_event(self, event):
        event.args.insert(0, self.uuid)
        return event

    def fire_this(self, event):
        event.args.insert(0, self.uuid)
        return self.fire(event)

    def peer_heartbeat(self):
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
                self.fire_this(PeerOffline())
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

    def peer_online(self, uuid):
        if uuid != self.uuid:
            return
        peer = self.peer
        if peer.is_offline:
            self.mark_online()

    def peer_offline(self, uuid):
        if uuid != self.uuid:
            return
        peer = self.peer
        if not peer.is_offline:
            self.mark_offline()

    def mark_online(self, startup=False):
        if not startup:
            logger.warning('Peer "%s" is ONLINE!', self.peer.hostname)
        peer = self.peer
        peer.is_offline = False
        peer.save()
        if hasattr(self, '_offline_timer'):
            self._offline_timer.unregister()
            delattr(self, '_offline_timer')
        self._heartbeat_timeout_count = 0

    def mark_offline(self):
        logger.error('Peer "%s" is OFFLINE!', self.peer.hostname)
        peer = self.peer
        peer.is_offline = True
        peer.save()
        self._offline_timer = Timer(60.0, PeerStillOffline(peer), persist=True).register(self)
        self.fire_this(PeerFailover())

    # nagnagnagnagnagnag
    def peer_still_offline(self, uuid):
        if uuid != self.uuid:
            return
        peer = self.peer
        logger.warning("Peer '%s' is STILL offline!", peer.hostname)

    #@handler('peer_discovered', channel='*')
    def peer_discovered(self, uuid, created=False):
        if uuid != self.uuid:
            return
        peer = self.peer
        #logger.info('Discovered peer "%s"', peer)
        if peer.is_offline:
            self.fire_this(PeerOnline())
        return True

    def peer_pool_health_check(self):
        peer = self.peer
        if peer.is_offline:
            return

        #def timeout(signum, frame):
        #    raise TimeoutError('Pool check on Peer "%s" timed out.' % peer.hostname)

        ## Set the signal handler and a 5-second alarm
        #signal.signal(signal.SIGALRM, timeout)
        #signal.alarm(5)

        #try:
        try:
            storage = peer.get_service('storage', default=None)
            ret = dict(storage.root.pools_health_check())
        except AttributeError:
            logger.error('Could not connect to Peer "%s"; is it offline and we don\'t know it yet?',
                         peer.hostname)
            return
        #logger.debug('Checking health of Pools "%s" on Peer "%s"', pools, peer.hostname)
        #retval = True
        for pool, is_healthy in ret.iteritems():
            if not is_healthy:
                self.fire_this(PeerPoolNotHealthy(pool))
                #retval = False
        #except TimeoutError, e:
        #    logger.error('Could not check pools on Peer "%s": %s',
        #                 peer.hostname, e.message)

        #signal.alarm(0)          # Disable the alarm
        #return retval
        return True

    def peer_pool_not_healthy(self, uuid, pool):
        if uuid != self.uuid:
            return
        peer = self.peer
        logger.error('Pool "%s" on Peer "%s" is NOT healthy!', pool, peer.hostname)

        # TODO If Pool has remote disks, and those are the only ones that are
        # missing, try a 'zpool clear'
