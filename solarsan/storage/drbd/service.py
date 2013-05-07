
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.template import quick_template
from solarsan.exceptions import DrbdResourceError
import sh
from ..volume import Volume
from .parsers import drbd_overview_parser
from .models import DrbdResource


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
        return drbd_overview_parser(resource=self.res.name) or {}

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
        return self.status().get('connection_state')

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
        return self.status().get('role')

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
        return self.status().get('disk_state')

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
        #return self.status().get('io_state')
        raise NotImplemented
