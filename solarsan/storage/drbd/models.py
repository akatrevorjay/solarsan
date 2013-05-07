
from solarsan import logging, signals
logger = logging.getLogger(__name__)
from solarsan import conf
#from solarsan.exceptions import DrbdError, DrbdResourceError
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.cluster.models import Peer
from random import getrandbits
from uuid import uuid4
import mongoengine as m
#import time
import weakref
from .util import drbd_find_free_minor
from .constants import DRBD_START_PORT


class DrbdPeer(m.EmbeddedDocument):
    peer = m.ReferenceField(Peer, dbref=False, required=True)
    minor = m.IntField(required=True)
    pool = m.StringField(required=True)
    volume = m.StringField(required=True)

    def __init__(self, **kwargs):
        super(DrbdPeer, self).__init__(**kwargs)

    def __repr__(self):
        return "<%s peer='%s', minor='%s', volume='%s'>" % (
            self.__class__.__name__, self.peer.hostname, self.minor, self.volume_full_name)

    def save(self, *args, **kwargs):
        if not self.minor:
            #self.minor = self.peer.rpc('drbd_find_free_minor')
            self.minor = drbd_find_free_minor()
        ret = super(DrbdPeer, self).save(*args, **kwargs)
        return ret

    @property
    def is_local(self):
        """Checks if this DrbdPeer is localhost"""
        return self.uuid == conf.config['uuid']

    @property
    def uuid(self):
        return self.peer.uuid

    @property
    def hostname(self):
        return self.peer.hostname

    @property
    def address(self):
        return self.peer.cluster_addr

    @property
    def port(self):
        return DRBD_START_PORT + self.minor

    @property
    def volume_full_name(self):
        return '%s/%s' % (self.pool, self.volume)

    @property
    def device(self):
        # Kinda hackish to hard code this when it's in
        # storage.volume.Volume.device
        return '/dev/zvol/%s' % self.volume_full_name

    """
    Service
    """
    _service = None

    @property
    def service(self):
        service = None
        if self._service:
            service = self._service()
        if service is None:
            if self.is_local:
                from .service import DrbdLocalResource
                service = DrbdLocalResource(self.volume)
            else:
                storage = self.peer.get_service('storage')
                service = storage.root.drbd_res_service(self.volume)
            self._service = weakref.ref(service)
        return service


#class DrbdResourceTargetMapping(m.EmbeddedDocument):
#    resource = m.ReferenceField(DrbdResource, dbref=False)
#    target = m.GenericReferenceField()
#    lun = m.IntField()


class DrbdResource(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    class signals:
        status_update = signals.resource_status_update
        connection_state_update = signals.resource_connection_state_update
        disk_state_update = signals.resource_disk_state_update
        role_update = signals.resource_role_update
        remote_disk_state_update = signals.resource_remote_disk_state_update
        remote_role_update = signals.resource_remote_role_update

    # Volumes are made with this name; the Drbd resource is also named this.
    name = m.StringField(unique=True, required=True)
    local = m.EmbeddedDocumentField(DrbdPeer)
    remote = m.EmbeddedDocumentField(DrbdPeer)
    shared_secret = m.StringField(required=True)
    sync_rate = m.StringField()
    size = m.StringField()
    uuid = m.UUIDField(binary=False)

    # status
    status = m.DictField()

    def __str__(self):
        #return self.name
        return self.__repr__().replace("'", '').replace("name=", '')

    def get_status(self, key):
        return self.status.get(key)

    def update_status(self, **kwargs):
        changed_kwargs = {}
        #orig_values = {}
        mongo_update = {}
        for k, v in kwargs.iteritems():
            orig_value = self.get_status(k)
            if orig_value != v:
                #self.update(**{'set__status__%s' % k: v})
                # TODO is this needed? shouldn't be.
                #self.status[key] = v
                changed_kwargs[k] = v
                mongo_update['set__status__%s' % k] = v
                #orig_values[k] = orig_value

        if mongo_update:
            self.update(**mongo_update)
        return changed_kwargs

        #self.update(**{'set__status': changed_kwargs})
        #if changed_kwargs:
        #    # doesn't seem to help for some reason?
        #    self.reload()
        #    self.signals.status_update.send(self, **changed_kwargs)
        #    for k, v in changed_kwargs.iteritems():
        #        signal = getattr(self.signals, k, None)
        #        if signal:
        #            signal.send(self, value=v, orig_value=orig_values[k])
        #return changed_kwargs
        #self.update(**{'set__status': kwargs})

    @property
    def connection_state(self):
        return self.get_status('connection_state')

    #@connection_state.setter
    #def connection_state(self, value):
    #    return self.update_status(connection_state=value)

    @property
    def disk_state(self):
        return self.get_status('disk_state')

    #@disk_state.setter
    #def disk_state(self, value):
    #    return self.update_status(disk_state=value)

    @property
    def role(self):
        return self.get_status('role')

    #@role.setter
    #def role(self, value):
    #    return self.update_status(role=value)

    @property
    def remote_disk_state(self):
        return self.get_status('remote_disk_state')

    #@remote_disk_state.setter
    #def remote_disk_state(self, value):
    #    return self.update_status(remote_disk_state=value)

    @property
    def remote_role(self):
        return self.get_status('remote_role')

    #@remote_role.setter
    #def remote_role(self, value):
    #    return self.update_status(remote_role=value)

    """
    Avoid possums
    """

    def __init__(self, **kwargs):
        super(DrbdResource, self).__init__(**kwargs)

    def save(self, *args, **kwargs):
        # Automatically generate a random secret if one was not specified
        # already
        if not self.shared_secret:
            self.generate_random_secret()
        if not self.uuid:
            self.uuid = uuid4()
        super(DrbdResource, self).save(*args, **kwargs)

    @property
    def peers(self):
        return [self.local, self.remote]

    def propogate_res_to_remote(self):
        """Heavy WIP, used to work, not sure if it does anymore"""
        logger.info('Propogating local Drbd Resource "%s" to Peer "%s".', self.name, self.remote.hostname)
        storage = self.remote.peer.get_service('storage')
        remote_res_obj = storage.root.drbd_res()
        remote_drbd_peer_obj = storage.root.drbd_res_peer()
        remote_peer_obj = storage.root.peer()

        res, created = remote_res_obj.objects.get_or_create(name=self.name)

        res.shared_secret = self.shared_secret
        res.sync_rate = self.sync_rate
        res.size = self.size

        res.local = remote_drbd_peer_obj(peer=remote_peer_obj.get_local(),
                                         minor=self.remote.minor,
                                         pool=self.remote.pool,
                                         volume=self.remote.volume,
                                         )

        res.remote = remote_drbd_peer_obj(peer=remote_peer_obj.objects.get(uuid=conf.config['uuid']),
                                          minor=self.local.minor,
                                          pool=self.local.pool,
                                          volume=self.local.volume,
                                          )

        res.save()

        return True

    @property
    def device(self):
        return '/dev/drbd/by-res/%s' % self.name

    def generate_random_secret(self):
        """Generates a random secret"""
        self.shared_secret = str(getrandbits(128))
        return True

    """
    Setup
    """

    is_initialized = m.BooleanField()

    @property
    def config_filename(self):
        return '/etc/drbd.d/%s.res' % self.name

    #def write_configs(self):
    #    """Generate a Drbd resource config file on Peers"""
    #    for peer in self.peers:
    #        peer.service.write_config()
    #    return True

    def initialize(self):
        for peer in self.peers:
            logger.info('Initializing Drbd Resource "%s" on Peer "%s".', self, peer)
            peer.service.initialize()
        return True
