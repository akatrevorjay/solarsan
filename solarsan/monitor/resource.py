
from solarsan.core import logger
from circuits import Component, Event, Timer, handler
from solarsan.storage.drbd import DrbdResource
import random


"""
Resource Manager
"""


class ResourceHealthCheck(Event):
    """Check Resource Health"""
    #complete = True


class ResourceManager(Component):
    #res_health_check_every = 10.0 + random.randrange(2, 10)
    res_health_check_every = 10.0

    def __init__(self):
        super(ResourceManager, self).__init__()
        self.resources = {}
        self.monitors = {}

        self.do_all_resources()
        self._health_check_timer = Timer(self.res_health_check_every, ResourceHealthCheck(), persist=True).register(self)

    def do_all_resources(self):
        for res in DrbdResource.objects.all():
            self.add_res(res)

    def add_res(self, res):
        if res.name in self.resources:
            return
        logger.info("Monitoring Resource '%s'.", res.name)
        self.resources[res.name] = res
        self.monitors[res.name] = ResourceMonitor(res).register(self)


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


class ResourceMonitor(Component):
    def __init__(self, res):
        super(ResourceMonitor, self).__init__()
        self.res = res
        self.status()

    @property
    def service(self):
        return self.res.local.service

    @handler('peer_failover', channel='*')
    def _on_peer_failover(self, peer):
        if peer.uuid != self.res.remote.peer.uuid:
            return

        # Update status and send out proper events first
        self.status()

        if self.res.role != 'Primary':
            logger.error('Failing over Peer "%s" for Resource "%s".',
                         self.res.remote.hostname, self.res.name)
            self.fire(ResourcePrimary(self.res))

    # TODO What exactly to do upon pool not healthy
    @handler('peer_pool_not_healthy', channel='*')
    def _on_peer_pool_not_healthy(self, peer, pool):
        if peer.uuid == self.res.local.peer.uuid and pool == self.res.local.pool:
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
            #self.fire(ResourceSecondary(self.res))

        elif peer.uuid == self.res.remote.uuid and pool == self.res.remote.pool:
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
            #self.fire(ResourcePrimary(self.res))

    """
    Status Tracking
    """

    def status(self):
        ret = self.service.status()
        if not ret:
            return ret

        events = []
        if ret['connection_state'] != self.res.connection_state:
            events.append(ResourceConnectionStateChange(self.res, ret['connection_state']))
        if ret['disk_state'] != self.res.disk_state:
            events.append(ResourceDiskStateChange(self.res, ret['disk_state']))
        if ret['role'] != self.res.role:
            events.append(ResourceRoleChange(self.res, ret['role']))
        if ret['remote_disk_state'] != self.res.remote_disk_state:
            events.append(ResourceRemoteDiskStateChange(self.res, ret['remote_disk_state']))
        if ret['remote_role'] != self.res.remote_role:
            events.append(ResourceRemoteRoleChange(self.res, ret['remote_role']))

        keys = ['connection_state', 'disk_state', 'role']
        keys += ['remote_' + x for x in keys[1:]]

        for k, v in ret.iteritems():
            if k in keys:
                setattr(self.res, k, v)
        self.res.save()

        for event in events:
            self.fire(event)

        return ret

    def resource_connection_state_change(self, res, cstate):
        if self.res.pk != res.pk:
            return
        logger.error("Resource '%s' is %s!", self.res.name, cstate)

        if cstate == 'Connected':
            """Connected. A DRBD connection has been established, data mirroring is now active. This is the
            normal state"""
            pass

        elif cstate == 'WFConnection':
            """WFConnection. This node is waiting until the peer node becomes visible on the network."""
            pass
            #if self.res.role != 'Primary':
            #    self.fire(ResourcePrimary(self.res))

        elif cstate == 'StandAlone':
            """StandAlone. No network configuration available. The resource has not yet been connected, or
            has been administratively disconnected (using drbdadm disconnect), or has dropped its connection
            due to failed authentication or split brain."""
            pass
            #if self.res.role != 'Primary':
            #    self.fire(ResourcePrimary(self.res))

        elif cstate == 'SyncSource':
            """SyncSource. Synchronization is currently running, with the local node being the source of
            synchronization"""
            pass
            #if self.res.role != 'Primary':
            #    self.fire(ResourcePrimary(self.res))

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

    def resource_disk_state_change(self, res, dstate):
        if self.res.pk != res.pk:
            return
        logger.error("Resource '%s' is %s!", self.res.name, dstate)

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

    def resource_role_change(self, res, role):
        if self.res.pk != res.pk:
            return
        logger.error("Resource '%s' is %s!", self.res.name, role)

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

    def resource_remote_disk_state_change(self, res, dstate):
        if self.res.pk != res.pk:
            return

    def resource_remote_role_change(self, res, remote_role):
        if self.res.pk != res.pk:
            return

    """
    Health Check
    """

    def resource_health_check(self):
        self.status()

        # If we have two secondary nodes become primary
        if self.res.role == 'Secondary' and self.res.remote_role == 'Secondary':
            logger.info('Taking over as Primary for Resource "%s" because someone has to. ' +
                        '(dual secondaries).', self.res.name)
            self.fire(ResourcePrimary(self.res))

    """
    Role Switching
    """

    def resource_primary(self, res):
        if res.pk != self.res.pk:
            return
        logger.warning('Promoting self to Primary for Resource "%s".', self.res.name)
        self.fire(ResourcePrimaryPre(self.res))
        self.fire(ResourcePrimaryTry(self.res))

    def resource_primary_try(self, res):
        if res.pk != self.res.pk:
            return

        self.status()
        if self.res.role == 'Primary':
            logger.error('Cannot promote self to Primary for Resource "%s", ' +
                         'as we\'re already Primary.', self.res.name)
            return
        if self.res.disk_state != 'UpToDate':
            logger.error('Cannot promote self to Primary for Resource "%s", ' +
                         'as disk state is not UpToDate.', self.res.name)
            return

        try:
            self.service.primary()
            self.status()
            logger.info('Promoted self to Primary for Resource "%s".', self.res.name)
            self.fire(ResourcePrimaryPost(self.res))
        except:
            retry_in = 10.0 + random.randrange(2, 10)
            logger.warning('Could not promote self to Primary for Resource "%s". Retrying in %ds.',
                           self.res.name, retry_in)
            self._primary_try_timer = Timer(retry_in, ResourcePrimaryTry(self.res))

    def resource_secondary(self, res, *values):
        if res.pk != self.res.pk:
            return

        self.status()
        if self.res.role == 'Secondary':
            return

        logger.warning('Demoting self to Secondary for Resource "%s".', self.res.name)
        self.fire(ResourceSecondaryPre(self.res))
        self.service.secondary()
        self.status()
        logger.warning('Demoted self to Secondary for Resource "%s".', self.res.name)
        self.fire(ResourceSecondaryPost(self.res))

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
