
from solarsan.core import logger
from solarsan import conf
#from solarsan.utils.exceptions import LoggedException
#from solarsan.template import quick_template

from storage.pool import Pool
from storage.volume import Volume
from storage.parsers.drbd import drbd_overview_parser
from storage.drbd import DrbdResource, DrbdPeer, drbd_find_free_minor
from solarsan.models import Config
from configure.models import Nic, get_all_local_ipv4_addrs
from cluster.models import Peer
import sh
import rpyc


class StorageService(rpyc.Service):
    def on_connect(self):
        logger.debug('Client connected.')

    def on_disconnect(self):
        logger.debug('Client disconnected.')

    def exposed_ping(self):
        return True

    """
    Pools
    """

    #def exposed_pool_create(self, name, vdevs):
    #    """Create Pool"""
    #    pass

    def exposed_pool_list(self, props=None):
        """List Pools"""
        return Pool.list(props=props, ret=dict, ret_obj=False)

    def exposed_pool_exists(self, pool):
        """Check if Pool exists"""
        pool = Pool(name=pool)
        return pool.exists()

    def exposed_pool_is_healthy(self, pool):
        """Check if Pool is healthy"""
        pool = Pool(name=pool)
        return pool.is_healthy()

    def exposed_pool_get_prop(self, pool, name, default=None):
        """Get property from Pool"""
        pool = Pool(name=pool)
        return pool.properties.get(name, default)

    def exposed_pool_set_prop(self, pool, name, value):
        """Set property to Pool"""
        pool = Pool(name=pool)
        pool.properties[name] = value
        return True

    #def exposed_pool_children_list(self, name):
    #    """List Pool children"""
    #    pool = Pool.objects.get(name=name)
    #    return pool.children

    """
    Volumes
    """

    def exposed_volume_create(self, volume, size, sparse=None, block_size=None):
        """Create Volume"""
        vol = Volume(name=volume)
        return vol.create(size, sparse=sparse, block_size=block_size)

    def exposed_volume_list(self, props=None):
        """List Volumes"""
        return Volume.list(props=props, ret=dict, ret_obj=False)

    def exposed_volume_exists(self, volume):
        """Check if Volume exists"""
        vol = Volume(name=volume)
        return vol.exists()

    def exposed_volume_get_prop(self, volume, name, default=None, source=None):
        """Get property from Volume"""
        vol = Volume(name=volume)
        return vol.properties.get(name, default, source=source)

    def exposed_volume_set_prop(self, volume, name, value):
        """Set property to vol"""
        vol = Volume(name=volume)
        vol.properties[name] = value
        return True

    """
    Cluster Probe
    """

    def exposed_peer_ping(self):
        """Pings Peer"""
        return True

    def exposed_peer_get_cluster_iface(self):
        """Gets Cluster IP"""
        config = Config.objects.get(name='cluster')
        iface = config.network_iface
        nic = Nic(str(iface))
        #nic_config = nic.config._data
        #nic_config.pop(None)

        ret = {
            'name': nic.name,
            'ipaddr': nic.ipaddr,
            'netmask': nic.netmask,
            'type': nic.type,
            'cidr': nic.cidr,
            'mac': nic.mac,
            'mtu': nic.mtu,
            #'config': nic_config,
        }
        return ret

    def exposed_peer_list_addrs(self):
        """Returns a list of IP addresses and network information"""
        return get_all_local_ipv4_addrs()

    def exposed_peer_hostname(self):
        """Returns hostname"""
        return conf.hostname

    """
    Replicated Volumes
    """

    def exposed_volume_repl_list(self, hostname=None):
        """Lists Cluster Volumes"""
        ret = []
        for line in sh.zfs('get', '-s', 'local', '-H', '-t', 'volume', 'solarsan:vol_repl',
                           _iter=True):
            line = line.rstrip("\n")
            name, prop, val, source = line.split("\t", 3)
            if val != 'true':
                logger.warning('Volume "%s" has vol_repl="%s" (not "true"). Ignoring property.')
                continue
            ret.append(name)
        return ret

    def exposed_drbd_res_list(self):
        """Lists DRBD replicated resources"""
        ret = []
        for res in DrbdResource.objects.all():
            ret.append(res.name)
        return ret

    def exposed_drbd_res_status(self, volume=None):
        """Get status of all DRBD replicated resources"""
        if volume:
            vol = Volume(name=Volume)
            return vol.replication_status
        else:
            return drbd_overview_parser()

    def exposed_drbd_res_setup(self, name, size, pool, peer_pool, peer_hostname):
        """Create new volume and setup Setup synchronous replication with a cluster peer."""
        local = Peer.get_local()
        remote = Peer.objects.get(hostname=peer_hostname)

        res = DrbdResource(name=name, size=size)
        res.peers.append(DrbdPeer(peer=local, pool=pool))
        res.peers.append(DrbdPeer(peer=remote, pool=peer_pool))

        # HACK
        res.local.minor = 2
        res.remote.minor = 2

        # Create volumes
        local('volume_create', res.local.volume_full_name, size)
        local('volume_set_prop', res.local.volume_full_name, 'solarsan:vol_repl', 'pending')
        remote('volume_create', res.remote.volume_full_name, size)
        remote('volume_set_prop', res.remote.volume_full_name, 'solarsan:vol_repl', 'pending')

        # Set properties
        #local('volume_set_prop', volume, 'solarsan:vol_repl_peer', remote.hostname)
        #remote('volume_set_prop', peer_volume, 'solarsan:vol_repl_peer', local.hostname)

        # TODO Use UUID per vol and peer

        # Write DRBD config on each box
        res = DrbdResource(name=name)
        local('drbd_res_write_config', res.name)
        remote('drbd_res_write_config', res.name)

    def exposed_drbd_res_write_config(self, resource, confirm=None):
        """Writes configuration for DrbdResource"""
        res = DrbdResource.objects.get(name=resource)
        return res.write_config(confirm=confirm)

    def exposed_drbd_res_create_md(self, resource):
        res = DrbdResource.objects.get(name=resource)
        return res.local.create_md()

    def exposed_drbd_find_free_minor(self):
        return drbd_find_free_minor()

    def exposed_drbd_primary(self, resource):
        logger.info("Got resource to promote: %s", resource)
        res = DrbdResource.objects.get(name=resource)
        res.promote_to_primary()

    """
    Target
    """

    #def exposed_target_scst_create_target(self, wwn, blah):
    #    pass

    def exposed_target_scst_status(self):
        return sh.service('scst', 'status')

    def exposed_target_scst_start(self):
        return sh.service('scst', 'start')

    def exposed_target_scst_stop(self):
        return sh.service('scst', 'stop')

    #def exposed_target_rts_status(self):
    #    pass
