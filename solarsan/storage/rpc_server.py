
from solarsan import logging, conf
logger = logging.getLogger(__name__)
#from solarsan.utils.exceptions import LoggedException
from .pool import Pool
from .volume import Volume
from .drbd import DrbdResource, DrbdPeer, DrbdLocalResource
from solarsan.configure.models import Nic, get_all_local_ipv4_addrs
from solarsan.cluster.models import Peer
#from solarsan.ha.models import FloatingIP
from netifaces import interfaces
import rpyc


class StorageService(rpyc.Service):
    #def on_connect(self):
    #    logger.debug('Client connected.')

    #def on_disconnect(self):
    #    logger.debug('Client disconnected.')

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
    Objects
    """

    def pool(self):
        return Pool

    def volume(self):
        return Volume

    def peer(self):
        return Peer

    """
    Replicated Volumes
    """

    def drbd_res(self):
        return DrbdResource

    def drbd_res_peer(self):
        return DrbdPeer

    def drbd_res_service(self, name):
        return DrbdLocalResource(name)

    """
    Peer Probe
    """

    def peer_get_cluster_iface(self):
        """Gets Cluster IP"""
        iface = conf.config['cluster_iface']
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

    def peer_uuid(self):
        """Returns UUID"""
        return conf.config['uuid']

    """
    Floating IP
    """

    def floating_ip_is_active(self, name):
        return name in interfaces()
        #ip = FloatingIP.objects.get(name=name)
        #return ip.is_active

    """
    Monitor
    """

    def pools_health_check(self):
        ret = {}
        pools = Pool.list(ret_obj=False, ret=dict)
        for name, pool in pools.iteritems():
            ret[name] = self.pool_health_check(name)
        return ret

    def pool_health_check(self, name):
        return Pool(name).is_healthy()


def main():
    from rpyc.utils.server import ThreadedServer

    from setproctitle import setproctitle
    title = 'SolarSan Storage'
    setproctitle('[%s]' % title)

    local = Peer.get_local()
    cluster_iface_bcast = local.cluster_nic.broadcast
    # Allow all public attrs, because exposed_ is stupid and should be a
    # fucking decorator.
    t = ThreadedServer(StorageService, port=18862,
                       registrar=rpyc.utils.registry.UDPRegistryClient(ip=cluster_iface_bcast,
                                                                       #logger=None,
                                                                       logger=logger,
                                                                       ),
                       auto_register=True,
                       #logger=logger.getChild('rpc.server_storage'),
                       logger=logger,
                       #logger=None,
                       protocol_config=conf.rpyc_conn_config)
    t.start()


if __name__ == '__main__':
    main()
