
from solarsan.core import logger
from solarsan import conf
from solarsan.models import Config, CreatedModifiedDocMixIn
from solarsan.configure.models import Nic

import mongoengine as m
import rpyc


"""
Cluster
"""


def get_cluster_config():
    created, ret = Config.objects.get_or_create(name='cluster')
    return ret


class Peer(m.Document, CreatedModifiedDocMixIn):
    #uuid = m.StringField(required=True, unique=True)
    hostname = m.StringField(required=True, unique=True)
    #is_local = m.BooleanField()

    ifaces = m.ListField()
    addrs = m.ListField()
    netmasks = m.ListField()

    # TODO Make this a list in desc priority or sort addrs by priority
    cluster_addr = m.StringField()
    cluster_iface = m.StringField()

    """
    General
    """

    def __init__(self, **kwargs):
        super(Peer, self).__init__(**kwargs)
        self._is_online = None
        self._lost_count = 0
        self._services = {}

    # TODO Manager
    @classmethod
    def get_local(cls):
        return Peer.objects.get(hostname=conf.hostname)

    @property
    def is_local(self):
        return self.hostname == conf.hostname

    @property
    def cluster_nic(self):
        return Nic(self.cluster_iface)

    """
    Newer RPC
    """

    @property
    def is_online(self):
        if self._is_online is None:
            try:
                self.storage.ping()
                self._is_online = True
            except:
                self._is_online = False
        return self._is_online

    def get_service(self, name):
        """Looks for service on Peer. Returns an existing one if possible, otherwise instantiates one."""
        name = str(name).upper()
        if name not in self._services or self._services[name].closed:
            #try:
            service = rpyc.connect_by_service(name, host=self.cluster_addr)
            #except Exception:
            #    raise
            for alias in service.root.get_service_aliases():
                self._services[alias] = service
        else:
            service = self._services[name]
        return service

    @property
    def storage(self):
        return self.get_service('storage')

    def __call__(self, method, *args, **kwargs):
        default_on_timeout = kwargs.pop('_default_on_timeout', 'exception')

        # TODO handle multiple service names
        service = self.get_service('storage')

        try:
            meth = getattr(service.root, method)
            ret = meth(*args, **kwargs)

            if not self.is_online:
                self.is_online = True
                self._lost_count = 0
            return ret
        except EOFError:
            if self.is_online:
                self.is_online = False
            self._lost_count += 1

            if default_on_timeout == 'exception':
                raise
            else:
                return default_on_timeout

    """
    Maybe..
    """

    def pools(self):
        return self('pool_list', _default_on_timeout={})

    def volumes(self):
        return self('volume_list', _default_on_timeout={})

    """
    HA Pri/Sec
    """

    def promote(self):
        # TODO Health checks to make sure uptodate first
        logger.info('Promoting all replicated volumes to primary')
        self.storage.root.drbd_res_primary()

        # TODO Write out new SCST config file, ALWAYS have SCST running, even
        # with an empty config. Just reload the config with scstadmin -config
        logger.info('Starting targets on "%s"', self.hostname)
        self.storage.root.target_scst_start()

    def demote(self):
        pass

    """
    State
    '''

    #STATES = {
    #    0: 'ONLINE',
    #    1: 'OFFLINE',
    #    #5: 'DEAD',
    #}

    #_state = m.DynamicField()
    ONLINE = True
    OFFLINE = False

    @apply
    def state():
        doc = '''Host State'''

        def fget(self):
            return self._state

        def fset(self, value):
            if self._state:
                logger.info('Peer "%s" changed state to "%s"', self.hostname, value)
            self._state = value

        def fdel(self):
            #delattr(self, '_state')
            pass

        return property(**locals())
    """
