
from solarsan.core import logger
from solarsan import conf
#from solarsan.utils.exceptions import LoggedException
from storage.pool import Pool
from storage.volume import Volume
from storage.drbd import DrbdResource, DrbdPeer, DrbdResourceService
from solarsan.models import Config
from configure.models import Nic, get_all_local_ipv4_addrs
from cluster.models import Peer
import sh
import rpyc


class Storage2Service(rpyc.Service):
    def on_connect(self):
        logger.debug('Client connected.')

    def on_disconnect(self):
        logger.debug('Client disconnected.')

    def ping(self):
        return True

    # Override the stupid prepending of expose_prefix to attrs, why is the
    # config not honored??
    def _rpyc_getattr(self, name):
        return getattr(self, name)

    #def _rpyc_delattr(self, name):
    #    pass

    #def _rpyc_setattr(self, name, value):
    #    pass

    """
    Cluster Probe
    """

    def peer_ping(self):
        """Pings Peer"""
        return True

    def peer_get_cluster_iface(self):
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

    def peer_list_addrs(self):
        """Returns a list of IP addresses and network information"""
        return get_all_local_ipv4_addrs()

    def peer_hostname(self):
        """Returns hostname"""
        return conf.hostname

    """
    Objects
    """

    def pool(self, name):
        return Pool.objects.get(name=name)

    def volume(self, name):
        return Volume.objects.get(name=name)

    def peer(self):
        return Peer

    """
    Replicated Volumes
    """

    def drbd_res(self):
        return DrbdResource

    def drbd_peer(self):
        return DrbdPeer

    def drbd_res_service(self, name):
        return DrbdResourceService(name)

    """
    Target
    """

    def target_scst_status(self):
        try:
            sh.service('scst', 'status')
            return True
        except:
            return False

    def target_scst_start(self):
        return sh.service('scst', 'start')

    def target_scst_stop(self):
        return sh.service('scst', 'stop')

    def target_scst_restart(self):
        return sh.service('scst', 'restart')

    def target_scst_reload_config(self):
        return sh.scstadmin('-config', '/etc/scst.conf')

    def target_scst_write_config(self):
        raise NotImplemented

    def target_scst_create_target(self, wwn, blah):
        raise NotImplemented

    #def target_rts_status(self):
    #    pass
