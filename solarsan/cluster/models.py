
from solarsan.core import logger
from solarsan import conf
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.configure.models import Nic
from solarsan.exceptions import ConnectionError
import mongoengine as m
import rpyc


"""
Cluster
"""


class Peer(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    uuid = m.StringField(required=True, unique=True)
    hostname = m.StringField(required=True, unique=True)
    #hostname = m.StringField(required=True, unique=True)
    #is_local = m.BooleanField()

    ifaces = m.ListField()
    addrs = m.ListField()
    netmasks = m.ListField()

    # TODO Make this a list in desc priority or sort addrs by priority
    cluster_addr = m.StringField()
    cluster_iface = m.StringField()

    last_seen = m.DateTimeField()
    is_offline = m.BooleanField()

    # For ReprMixIn
    _repr_vars = ['hostname']

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
        ret, created = Peer.objects.get_or_create(uuid=conf.config['uuid'], defaults={'hostname': conf.hostname})
        ret.hostname = conf.hostname
        ret.uuid = conf.config['uuid']
        ret.cluster_iface = conf.config['cluster_iface']
        return ret

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

    def get_service(self, name, default='exception', cache=True):
        """Looks for service on Peer. Returns an existing one if possible, otherwise instantiates one."""
        NAME = str(name).upper()
        check_service = None
        if cache:
            check_service = self._services.get(NAME)
        if not check_service or check_service.closed:
            service = None
            try:
                service = rpyc.connect_by_service(NAME, host=self.cluster_addr)
                                                  #config=conf.rpyc_conn_config)

                # Remove existing aliases to old service
                if cache:
                    if check_service:
                        for alias in self._services.keys():
                            if self._services[alias] == check_service:
                                self._services.pop(alias)

                    # Add in the new service's
                    for alias in service.root.get_service_aliases():
                        self._services[alias] = service
            except Exception, e:
                if check_service:
                    check_service.close()
                if service:
                    service.close()
                if default == 'exception':
                    raise ConnectionError('Cannot get service "%s": "%s"', name, e.message)
                else:
                    return default
        else:
            service = self._services[NAME]
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
                self._is_online = True
                self._lost_count = 0
            return ret
        except EOFError:
            if self.is_online:
                self._is_online = False
            self._lost_count += 1

            if default_on_timeout == 'exception':
                raise
            else:
                return default_on_timeout
