
from solarsan.core import logger
from solarsan import conf
from solarsan.models import Config, CreatedModifiedDocMixIn
from solarsan.configure.models import Nic
from solarsan.exceptions import ConnectionError
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

    last_seen = m.DateTimeField()
    is_offline = m.BooleanField()

    """
    General
    """

    def __init__(self, **kwargs):
        super(Peer, self).__init__(**kwargs)
        self._is_online = None
        self._lost_count = 0
        self._services = {}

        """ THIS NEEDS TO BE DYNAMIC THIS IS A HUGE HACK AND WONT WORK """
        self.is_primary = False

    def __repr__(self):
        append = ''
        if self.hostname:
            append += " hostname='%s'" % self.hostname
        return '<%s%s>' % (self.__class__.__name__, append)

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

    def get_service(self, name, default='exception'):
        """Looks for service on Peer. Returns an existing one if possible, otherwise instantiates one."""
        NAME = str(name).upper()
        check_service = self._services.get(NAME)
        if not check_service or check_service.closed:
            service = None
            try:
                service = rpyc.connect_by_service(NAME, host=self.cluster_addr)
                                                  #config=conf.rpyc_conn_config)

                # Remove existing aliases to old service
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

    def failover_for_peer(self, peer):
        if self.is_primary:
            raise Exception("Already primary!")

        """
        Failover crap that needs moved to signals on Peer online/offline

        TODO Check DRBD status before failing over
            - If no connection has been seen for a while over multiple
              interfaces, become primary
            - If connetion is still up, but peer appears crashed, wait some
              amount of time for watchdog to hopefully kick in and reboot that
              shit to let you do your thang
            - If other peer is currently marked as primary, we can't fucking do
              this so don't try to.
            - If we are not UpToDate, then definitely do NOT become primary
        """

        logger.error("Failing over for peer '%s'", peer.hostname)

        # TODO rpyc of objects and per peer filter
        logger.info('Getting list of resources')
        for res in self('drbd_res_list'):
            # TODO signal going primary on res
            logger.info('Primary on "%s"', res)
            self('drbd_primary', res)

        # TODO rewrite SCST config
        logger.info('TODO Rewriting SCST config')

        # TODO signal about to reload SCST config
        if not self('target_scst_status'):
            logger.info('Starting SCST')
            self('target_scst_start')
        else:
            logger.info('Reloading SCST config')
            self('target_scst_reload_config')

        # TODO HA IP lookup from Config, use signals to run this
        # shit on down or up
        logger.info('TODO Taking over HA IP')

        self.is_primary = True

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
