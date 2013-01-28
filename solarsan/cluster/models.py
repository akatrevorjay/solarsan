
from solarsan.core import logger
from solarsan.rpc.client import StorageClient
from solarsan.utils.cache import cached_property
#from ..utils.cache import cached_property
#from ..utils.stack import get_current_func_name

import mongoengine as m
from datetime import datetime
from socket import gethostname
import zerorpc


"""
Cluster
"""


class Peer(m.Document):
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
    Created/Modified
    """

    created = m.DateTimeField(default=datetime.utcnow())
    modified = m.DateTimeField(default=datetime.utcnow())

    def save(self, *args, **kwargs):
        """Overrides save for created and modified properties"""
        if not self.pk:
            self.created = datetime.utcnow()
        if self._changed_fields:
            self.modified = datetime.utcnow()
        super(Peer, self).save(*args, **kwargs)

    """
    General
    """

    def __init__(self, **kwargs):
        super(Peer, self).__init__(**kwargs)
        self.is_online = True
        self._lost_count = 0

    # TODO Manager
    @classmethod
    def get_local(cls):
        return Peer.objects.get(hostname=gethostname())

    @property
    def is_local(self):
        return gethostname() == self.hostname

    """
    Newer RPC
    """

    @property
    def storage(self):
        if not hasattr(self, '_storage'):
            self._storage = StorageClient(self.cluster_addr)
        return self._storage

    def __call__(self, method, *args, **kwargs):
        default_on_timeout = kwargs.pop('_default_on_timeout', 'exception')

        try:
            ret = self.storage(method, *args, **kwargs)

            if not self.is_online:
                self.is_online = True
                self._lost_count = 0
            return ret
        except (zerorpc.TimeoutExpired, zerorpc.LostRemote), e:
            if self.is_online:
                self.is_online = False
            self._lost_count += 1

            if default_on_timeout == 'exception':
                raise e
            else:
                return default_on_timeout

    """
    Cached props
    """

    @cached_property(ttl=10)
    def pools(self):
        return self('pool_list', _default_on_timeout={})

    @cached_property(ttl=10)
    def volumes(self):
        return self('volume_list', _default_on_timeout={})

    """
    HA Pri/Sec
    """

    def promote(self):
        # TODO Health checks to make sure uptodate
        logger.info('Promoting all replicated volumes to primary')
        self('volume_repl_promote')
        logger.info('Starting targets on "%s"', self.hostname)
        self('target_start')

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
