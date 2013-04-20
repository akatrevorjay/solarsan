
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer, handler
from solarsan.storage.drbd import DrbdResource
import random
import weakref


"""
Resource Manager
"""


class ResourceHealthCheck(Event):
    """Check Resource Health"""
    #complete = True


class ResourceManager(Component):
    channel = 'resource'
    #health_check_every = 10.0 + random.randrange(2, 10)
    #health_check_every = 120.0
    health_check_every = 10.0

    def __init__(self):
        super(ResourceManager, self).__init__()
        self.monitors = {}

        self._health_check_timer = Timer(self.health_check_every,
                                         ResourceHealthCheck(),
                                         self.channel,
                                         persist=True,
                                         ).register(self)

    #@handler('managers_check', channel='*')
    def managers_check(self):
        uuids = []
        for res in DrbdResource.objects.all():
            self.add_res(res)
            uuids.append(res.uuid)
        for uuid in self.monitors.keys():
            if uuid not in uuids:
                self.monitors[uuid].unregister()
                self.monitors.pop(uuid)
        self.fire(ResourceHealthCheck())

    def add_res(self, res):
        if res.uuid in self.monitors:
            return

        #self.monitors[res.uuid] = ResourceMonitor(res.uuid, channel='resource-%s' % res.uuid).register(self)
        self.monitors[res.uuid] = ResourceMonitor(res.uuid).register(self)


"""
Resource Monitor
"""


class ResourcePrimaryPre(Event):
    """Promote to Primary"""


class ResourcePrimary(Event):
    """Promote to Primary"""


class ResourcePrimaryTry(Event):
    """Promote to Primary Try"""


class ResourcePrimaryPost(Event):
    """Promote to Primary"""


class ResourceSecondaryPre(Event):
    """Demote to Secondary"""


class ResourceSecondary(Event):
    """Demote to Secondary"""


class ResourceSecondaryPost(Event):
    """Demote to Secondary"""


class ResourceConnectionStateChange(Event):
    """Resource Connection State Change"""


class ResourceDiskStateChange(Event):
    """Resource Disk State Change"""


class ResourceRoleChange(Event):
    """Resource Connection State Change"""
    #success = True
    #complete = True


class ResourceRemoteDiskStateChange(Event):
    """Resource Remote Disk State Change"""


class ResourceRemoteRoleChange(Event):
    """Resource Remote Role State Change"""


def get_resource(uuid):
    return DrbdResource.objects.get(uuid=uuid)


class ResourceMonitor(Component):
    channel = 'resource'
    uuid = None

    def __init__(self, uuid, channel=channel):
        self.uuid = uuid
        super(ResourceMonitor, self).__init__(channel=channel)

        res = self.res
        logger.info("Monitoring Resource '%s'.", res.name)

        uuids = self.peer_uuids = []
        peers = self.peers = {}
        for peer in res.peers:
            uuids.append(peer.uuid)
            peers[peer.uuid] = dict(is_local=peer.is_local, hostname=peer.hostname, pool=peer.pool)
        self.peer_local_uuid = res.local.uuid

        self.update_status(send_events=False)

    def get_res(self):
        return get_resource(self.uuid)

    _res = None

    #@property
    #def res(self):
    #    if self._res:
    #        self._res.reload()
    #    else:
    #        try:
    #            self._res = self.get_res()
    #        except DrbdResource.DoesNotExist:
    #            logger.error('Resource with uuid=%s does not exist anymore', self.uuid)
    #            self.unregister()
    #    return self._res

    @property
    def res(self):
        res = None
        if self._res:
            res = self._res()
        if res is not None:
            res.reload()
        else:
            try:
                res = self.get_res()
            except DrbdResource.DoesNotExist:
                logger.error('Resource with uuid=%s does not exist anymore', self.uuid)
                self.unregister()
            self._res = weakref.ref(res)
        return res

    def get_event(self, event):
        event.args.insert(0, self.uuid)
        return event

    def fire_this(self, event):
        #return self.fire(self.get_event(event), self.channel)
        #return self.fire(self.get_event(event), self)
        return self.fire(self.get_event(event))

    peers = None
    peer_uuids = None
    peer_local_uuid = None

    @property
    def peer_local(self):
        return self.peers.get(self.peer_local_uuid)

    #def get_peer(self, index=None, local=False, remote=False, res=None):
    #    if not res:
    #        res = self.get_res()
    #    if local:
    #        return res.local
    #    if remote:
    #        return res.remote
    #    if index:
    #        return res.peers[index]
    #    return res.peers

    @property
    def service(self):
        return self.res.local.service

    @handler('peer_failover', channel='peer')
    def _on_peer_failover(self, peer_uuid):
        if peer_uuid not in self.peers or peer_uuid == self.peer_local_uuid:
            return

        # Update status and send out proper events first
        self.update_status()

        res = self.res

        if res.role != 'Primary':
            logger.error('Failing over Peer "%s" for Resource "%s".',
                         res.remote.hostname, res.name)
            self.fire_this(ResourcePrimary())

    # TODO What exactly to do upon pool not healthy
    @handler('peer_pool_not_healthy', channel='peer')
    def _on_peer_pool_not_healthy(self, peer_uuid, pool):
        if peer_uuid not in self.peers or pool != self.peers[peer_uuid]['pool']:
            return

        if peer_uuid == self.peer_local_uuid:
            #logger.error('Pool "%s" for Resource "%s" is not healthy.',
            #             pool, self.res.name)
            pass

            ## TODO Remove any HA IPs for this resource (handled by the target,
            # nothing here)
            #   - But what if the HA IPs are used for others? HA IPs must be
            #   attached to targets, and only whole targets can go up or down.
            # TODO Remove any targets for this resource
            #   - But what if the targets are used for other resources? Again,
            #   must do a whole target at a time, up or down.
            # TODO Secondary ourselves
            #self.fire_this(ResourceSecondary())

        else:
            #logger.error('Pool "%s" on Remote "%s" for Resource "%s" is not healthy.',
            #             pool, self.res.remote.hostname, self.res.name)
            pass

            # TODO Primary ourselves
            # TODO Add any targets for this resource
            #   - But what if the targets are used for other resources? Again,
            #   must do a whole target at a time, up or down.
            # TODO Add any HA IPs for that target (handled by the target,
            # nothing here)
            #   - But what if the HA IPs are used for others? HA IPs must be
            #   attached to targets, and only whole targets can go up or down.
            #self.fire_this(ResourcePrimary())

    """
    Status Tracking
    """

    _update_status_event_map = {
        'connection_state': ResourceConnectionStateChange,
        'disk_state': ResourceDiskStateChange,
        'role': ResourceRoleChange,
        'remote_disk_state': ResourceRemoteDiskStateChange,
        'remote_role': ResourceRemoteRoleChange,
    }

    def update_status(self, send_events=True):
        #if self.uuid != uuid:
        #    return

        ret = dict(self.service.status())
        #logger.info('update_status ret=%s', ret)
        if not ret:
            return

        res = self.res
        changed_kwargs = res.update_status(**ret)
        #res.reload()

        if send_events and changed_kwargs:
            logger.debug('update_status changed=%s', changed_kwargs)
            for k, v in changed_kwargs.iteritems():
                event_cls = self._update_status_event_map.get(k)
                if event_cls:
                    self.fire_this(event_cls(v))

        return changed_kwargs

    def resource_connection_state_change(self, uuid, cstate):
        if self.uuid != uuid:
            return
        res = self.res
        logger.error("Resource '%s' is %s!", res.name, cstate)

        if cstate == 'Connected':
            """Connected. A DRBD connection has been established, data mirroring is now active. This is the
            normal state"""
            pass

        elif cstate == 'WFConnection':
            """WFConnection. This node is waiting until the peer node becomes visible on the network."""
            pass
            #if res.role != 'Primary':
            #    self.fire_this(ResourcePrimary())

        elif cstate == 'StandAlone':
            """StandAlone. No network configuration available. The resource has not yet been connected, or
            has been administratively disconnected (using drbdadm disconnect), or has dropped its connection
            due to failed authentication or split brain."""
            pass
            #if res.role != 'Primary':
            #    self.fire_this(ResourcePrimary())

        elif cstate == 'SyncSource':
            """SyncSource. Synchronization is currently running, with the local node being the source of
            synchronization"""
            pass
            #if res.role != 'Primary':
            #    self.fire_this(ResourcePrimary())

        elif cstate == 'PausedSyncS':
            """PausedSyncS. The local node is the source of an ongoing synchronization, but synchronization
            is currently paused. This may be due to a dependency on the completion of another synchronization
            process, or due to synchronization having been manually interrupted by drbdadm pause-sync."""
            pass

        elif cstate == 'SyncTarget':
            """SyncTarget. Synchronization is currently running, with the local node being the target of
            synchronization."""
            pass

        elif cstate == 'PausedSyncT':
            """PausedSyncT. The local node is the target of an ongoing synchronization, but synchronization
            is currently paused. This may be due to a dependency on the completion of another synchronization
            process, or due to synchronization having been manually interrupted by drbdadm pause-sync."""
            pass

        elif cstate == 'VerifyS':
            """VerifyS. On-line device verification is currently running, with the local node being the source
            of verification."""
            pass

        elif cstate == 'VerifyT':
            """VerifyT. On-line device verification is currently running, with the local node being the target
            of verification."""
            pass

        else:
            raise Exception('Resource "%s" has an unknown connection state of "%s"!', cstate)

    def resource_disk_state_change(self, uuid, dstate):
        if self.uuid != uuid:
            return
        res = self.res
        logger.error("Resource '%s' is %s!", res.name, dstate)

        if dstate == 'UpToDate':
            """UpToDate. Consistent, up-to-date state of the data. This is the normal state."""

        elif dstate == 'Diskless':
            """Diskless. No local block device has been assigned to the DRBD driver. This may mean that the
            resource has never attached to its backing device, that it has been manually detached using
            drbdadm detach, or that it automatically detached after a lower-level I/O error."""
            pass

        elif dstate == 'Inconsistent':
            """Inconsistent. The data is inconsistent. This status occurs immediately upon creation of a new
            resource, on both nodes (before the initial full sync). Also, this status is found in one node
            (the synchronization target) during synchronization."""
            pass

        elif dstate == 'Outdated':
            """Outdated. Resource data is consistent, but outdated."""
            pass

        elif dstate == 'DUnknown':
            """DUnknown. This state is used for the peer disk if no network connection is available."""
            pass

        elif dstate == 'Consistent':
            """Consistent. Consistent data of a node without connection. When the connection is established,
            it is decided whether the data is UpToDate or Outdated."""
            pass

        else:
            raise Exception('Resource "%s" has an unknown disk state of "%s"!', dstate)

    def resource_role_change(self, uuid, role):
        if self.uuid != uuid:
            return
        res = self.res
        logger.error("Resource '%s' is %s!", res.name, role)

        if role == 'Primary':
            """Primary. The resource is currently in the primary role, and may be read from and written to.
            This role only occurs on one of the two nodes, unless dual-primary mode is enabled."""
            pass

        elif role == 'Secondary':
            """Secondary. The resource is currently in the secondary role. It normally receives updates from
            its peer (unless running in disconnected mode), but may neither be read from nor written to.
            This role may occur on one or both nodes."""
            pass

        elif role == 'Unknown':
            """Unknown. The resource's role is currently unknown. The local resource role never has this
            status. It is only displayed for the peer's resource role, and only in disconnected mode."""
            pass

        else:
            raise Exception('Resource "%s" has an unknown role of "%s"!', role)

    #def resource_remote_disk_state_change(self, uuid, dstate):
    #    if self.uuid != uuid:
    #        return
    #    res = self.res

    #def resource_remote_role_change(self, uuid, remote_role):
    #    if self.uuid != uuid:
    #        return
    #    res = self.res

    """
    Health Check
    """

    def resource_health_check(self):
        if self.update_status():
            yield None

        res = self.res
        #logger.debug("Resource '%s' health check", res.name)
        #logger.debug("Resource '%s' status=%s", res.name, res.status)

        # If we have two secondary nodes become primary
        if res.role == 'Secondary' and res.remote_role == 'Secondary':
            logger.info('Taking over as Primary for Resource "%s" because someone has to. ' +
                        '(dual secondaries).', res.name)
            self.fire_this(ResourcePrimary())

    """
    Role Switching
    """

    def resource_primary(self, uuid):
        if self.uuid != uuid:
            return
        res = self.res

        logger.warning('Promoting self to Primary for Resource "%s".', res.name)
        self.fire_this(ResourcePrimaryPre())
        yield None
        self.fire_this(ResourcePrimaryTry())

    def resource_primary_try(self, uuid):
        if self.uuid != uuid:
            return

        if self.update_status():
            yield None

        res = self.res

        #self.status()
        if res.role == 'Primary':
            logger.error('Cannot promote self to Primary for Resource "%s", ' +
                         'as we\'re already Primary.', res.name)
            return
        if res.disk_state != 'UpToDate':
            logger.error('Cannot promote self to Primary for Resource "%s", ' +
                         'as disk state is not UpToDate.', res.name)
            return

        try:
            self.service.primary()

            if self.update_status():
                yield None

            logger.info('Promoted self to Primary for Resource "%s".', res.name)

            self.fire_this(ResourcePrimaryPost())
        except:
            retry_in = 10.0 + random.randrange(2, 10)
            logger.warning('Could not promote self to Primary for Resource "%s". Retrying in %ds.',
                           res.name, retry_in)
            self._primary_try_timer = Timer(retry_in, self.get_event(ResourcePrimaryTry()), self.channel)

    def resource_secondary(self, uuid, *values):
        if self.uuid != uuid:
            return

        if self.update_status():
            yield None

        res = self.res

        if res.role == 'Secondary':
            return

        logger.warning('Demoting self to Secondary for Resource "%s".', res.name)
        self.fire_this(ResourceSecondaryPre())
        self.service.secondary()

        if self.update_status():
            yield None
        #del res
        res = self.res

        logger.warning('Demoted self to Secondary for Resource "%s".', res.name)
        self.fire_this(ResourceSecondaryPost())

    """
    TODO
    """

    #def get_primary(self):
    #    if self._status['role'] == 'Primary':
    #        return self.res.local
    #    elif self._status['remote_role'] == 'Primary':
    #        return self.res.remote

    def write_config(self, adjust=True):
        self.service.write_config()
        if adjust:
            self.service.adjust()
        return True
