
from solarsan import logging, signals
logger = logging.getLogger(__name__)
from solarsan import conf
from solarsan.template import quick_template
from solarsan.exceptions import DrbdResourceError
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.cluster.models import Peer
from .volume import Volume
from .parsers.drbd import drbd_overview_parser
from random import getrandbits
from uuid import uuid4
import mongoengine as m
import sh
#import time
import weakref


def drbd_find_free_minor():
    ov = drbd_overview_parser()
    minors = [int(x['minor']) for x in ov.values()]
    for minor in range(0, 9):
        if minor not in minors:
            return minor
    raise Exception("Could not find a free minor available")


DRBD_START_PORT = 7800


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


"""
TODO Drbd Handlers

pri-on-incon-degr, pri-lost-after-sb, pri-lost, fence-peer (formerly oudate-peer), local-io-error, initial-split-brain, split-brain, before-resync-target, after-resync-target

Env vars for ran scripts:
    DRBD_RESOURCE is the name of the resource
    DRBD_MINOR is the minor number of the DRBD device, in decimal.
    DRBD_CONF is the path to the primary configuration file; if you split your configuration into multiple files (e.g. in /etc/drbd.conf.d/), this will not be helpful.
    DRBD_PEER_AF , DRBD_PEER_ADDRESS , DRBD_PEERS are the address family (e.g. ipv6), the peer's address and hostnames.
"""


class DrbdLocalResource(object):
    def __init__(self, resource_name):
        self.res = DrbdResource.objects.get(name=resource_name)

    """
    Commands
    """

    def connect(self, discard_my_data=False):
        """Connect to peer.
        If discard_my_data=True, it will overwrite any local changes to force it to match peer.
        """
        args = []
        if discard_my_data:
            args.extend(['--', '--discard-my-data'])
        args.extend(['connect', self.res.name])
        sh.drbdadm(*args)
        return True

    def disconnect(self):
        """Disconnect from peer"""
        sh.drbdadm('disconnect', self.res.name)
        return True

    def primary(self):
        """Promote self to primary"""
        sh.drbdadm('primary', self.res.name)
        return True

    def secondary(self):
        """Demote self to secondary"""
        sh.drbdadm('secondary', self.res.name)
        return True

    def adjust(self):
        """Applies any config changes on resource"""
        sh.drbdadm('adjust', self.res.name)
        return True

    def outdate(self):
        """Outdates resource"""
        sh.drbdadm('outdate', self.res.name)
        return True

    def resize(self, assume_clean=False):
        """Dynamically grow the Resource to be the full size of the underlying Volume.
        If assume_clean=True, then it assumes the new space is just new space, not existing data,
        that way it doesn't have to sync it between peers."""
        args = []
        if assume_clean:
            args.extend(['--', '--assume_clean'])
        args.extend(['resize', self.res.name])
        sh.drbdadm(*args)
        return True

    """
    Initialize
    """

    def initialize(self):
        if self.res.is_initialized:
            raise DrbdResourceError('Resource "%s" is already initialized.', self.res)
        logger.info('Initializing Resource "%s".', self.res)

        volume_name = self.res.volume_full_name
        logger.info('Creating Volume "%s" for Resource "%s".', volume_name, self.res)
        vol = Volume(name=volume_name)
        vol.create(self.res.size)

        logger.info('Creating Metadata for Resource "%s".', self.res)
        sh.drbdadm('create-md', self.res.name)

        logger.info('Writing config for Resource "%s".', self.res)
        self.write_config()

        logger.info('Reloading Drbd for Resource "%s".', self.res)
        self.adjust()

        logger.info('Marking Resource "%s" as initialized.', self.res)
        self.res.is_initialized = True
        self.res.save()

        logger.info('Resource "%s" has been initialized.', self.res)
        return True

    """
    Config
    """

    def write_config(self):
        """Writes configuration for DrbdResource"""
        context = {'res': self.res}
        fn = self.res.config_filename
        logger.info('Writing config for Resource "%s" to "%s".', self.res, fn)
        quick_template('drbd-resource.conf', context=context, write_file=fn)
        return True

    """
    Status
    """

    def status(self):
        return drbd_overview_parser(resource=self.res.name)

    #@property
    #def replication_is_healthy(self):
    #    if self.disk_state == 'UpToDate' and self.connection_state == 'Connected':
    #        return True
    #    else:
    #        return False

    """
    Connection State

    A resource's connection state can be observed either by monitoring /proc/drbd, or by issuing the drbdadm cstate command:

    # drbdadm cstate <resource>
    Connected
    A resource may have one of the following connection states:

    StandAlone. No network configuration available. The resource has not yet been connected, or has been administratively disconnected (using drbdadm disconnect), or has dropped its connection due to failed authentication or split brain.

    Disconnecting. Temporary state during disconnection. The next state is StandAlone.

    Unconnected. Temporary state, prior to a connection attempt. Possible next states: WFConnection and WFReportParams.

    Timeout. Temporary state following a timeout in the communication with the peer. Next state: Unconnected.

    BrokenPipe. Temporary state after the connection to the peer was lost. Next state: Unconnected.

    NetworkFailure. Temporary state after the connection to the partner was lost. Next state: Unconnected.

    ProtocolError. Temporary state after the connection to the partner was lost. Next state: Unconnected.

    TearDown. Temporary state. The peer is closing the connection. Next state: Unconnected.

    WFConnection. This node is waiting until the peer node becomes visible on the network.

    WFReportParams. TCP connection has been established, this node waits for the first network packet from the peer.

    Connected. A DRBD connection has been established, data mirroring is now active. This is the normal state.

    StartingSyncS. Full synchronization, initiated by the administrator, is just starting. The next possible states are: SyncSource or PausedSyncS.

    StartingSyncT. Full synchronization, initiated by the administrator, is just starting. Next state: WFSyncUUID.

    WFBitMapS. Partial synchronization is just starting. Next possible states: SyncSource or PausedSyncS.

    WFBitMapT. Partial synchronization is just starting. Next possible state: WFSyncUUID.

    WFSyncUUID. Synchronization is about to begin. Next possible states: SyncTarget or PausedSyncT.

    SyncSource. Synchronization is currently running, with the local node being the source of synchronization.

    SyncTarget. Synchronization is currently running, with the local node being the target of synchronization.

    PausedSyncS. The local node is the source of an ongoing synchronization, but synchronization is currently paused. This may be due to a dependency on the completion of another synchronization process, or due to synchronization having been manually interrupted by drbdadm pause-sync.

    PausedSyncT. The local node is the target of an ongoing synchronization, but synchronization is currently paused. This may be due to a dependency on the completion of another synchronization process, or due to synchronization having been manually interrupted by drbdadm pause-sync.

    VerifyS. On-line device verification is currently running, with the local node being the source of verification.

    VerifyT. On-line device verification is currently running, with the local node being the target of verification.
    """

    @property
    def connection_state(self):
        return self.status()['connection_state']

    #@property
    #def is_connected(self):
    #    return self.connection_state == 'Connected'

    #@property
    #def is_disconnected(self):
    #    """This probably shouldn't exist"""
    #    return not self.connection_state == 'Connected'

    """
    Role

    A resource's role can be observed either by monitoring /proc/drbd, or by issuing the drbdadm role command:

    # drbdadm role <resource>
    Primary/Secondary
    The local resource role is always displayed first, the remote resource role last.

    You may see one of the following resource roles:

    Primary. The resource is currently in the primary role, and may be read from and written to. This role only occurs on one of the two nodes, unless dual-primary mode is enabled.

    Secondary. The resource is currently in the secondary role. It normally receives updates from its peer (unless running in disconnected mode), but may neither be read from nor written to. This role may occur on one or both nodes.

    Unknown. The resource's role is currently unknown. The local resource role never has this status. It is only displayed for the peer's resource role, and only in disconnected mode.
    """

    @property
    def role(self):
        return self.status()['role']

    @property
    def is_primary(self):
        return self.role == 'Primary'

    #@property
    #def is_secondary(self):
    #    return self.role == 'Secondary'

    """
    Disk State

    A resource's disk state can be observed either by monitoring /proc/drbd, or by issuing the drbdadm dstate command:

    # drbdadm dstate <resource>
    UpToDate/UpToDate
    The local disk state is always displayed first, the remote disk state last.

    Both the local and the remote disk state may be one of the following:

    Diskless. No local block device has been assigned to the DRBD driver. This may mean that the resource has never attached to its backing device, that it has been manually detached using drbdadm detach, or that it automatically detached after a lower-level I/O error.

    Attaching. Transient state while reading meta data.

    Failed. Transient state following an I/O failure report by the local block device. Next state: Diskless.

    Negotiating. Transient state when an Attach is carried out on an already-Connected DRBD device.

    Inconsistent. The data is inconsistent. This status occurs immediately upon creation of a new resource, on both nodes (before the initial full sync). Also, this status is found in one node (the synchronization target) during synchronization.

    Outdated. Resource data is consistent, but outdated.

    DUnknown. This state is used for the peer disk if no network connection is available.

    Consistent. Consistent data of a node without connection. When the connection is established, it is decided whether the data is UpToDate or Outdated.

    UpToDate. Consistent, up-to-date state of the data. This is the normal state.
    """

    @property
    def disk_state(self):
        return self.status()['disk_state']

    @property
    def is_up_to_date(self):
        return self.disk_state == 'UpToDate'

    """
    IO State Flags

    The I/O state flag field in /proc/drbd contains information about the current state of I/O operations associated with the resource. There are six such flags in total, with the following possible values:

    I/O suspension. Either r for running or s for suspended I/O. Normally r.
    Serial resynchronization. When a resource is awaiting resynchronization, but has deferred this because of a resync-after dependency, this flag becomes a. Normally -.
    Peer-initiated sync suspension. When resource is awaiting resynchronization, but the peer node has suspended it for any reason, this flag becomes p. Normally -.
    Locally initiated sync suspension. When resource is awaiting resynchronization, but a user on the local node has suspended it, this flag becomes u. Normally -.
    Locally blocked I/O. Normally -. May be one of the following flags:

    d: I/O blocked for a reason internal to DRBD, such as a transient disk state.
    b: Backing device I/O is blocking.
    n: Congestion on the network socket.
    a: Simultaneous combination of blocking device I/O and network congestion.
    Activity Log update suspension. When updates to the Activity Log are suspended, this flag becomes s. Normally -.
    """

    @property
    def io_state(self):
        #return self.status()['io_state']
        raise NotImplemented
