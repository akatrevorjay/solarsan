
from solarsan.core import logger
from solarsan import conf
#from solarsan.utils.exceptions import LoggedException
from storage.pool import Pool
from storage.volume import Volume
from storage.drbd import DrbdResource, DrbdPeer, DrbdResourceService, drbd_find_free_minor
from storage.parsers.drbd import drbd_overview_parser
from configure.models import Nic, get_all_local_ipv4_addrs
from cluster.models import Peer
#from ha.models import ActivePassiveIP
from netifaces import interfaces
import rpyc


class StorageService(rpyc.Service):
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
        return DrbdResourceService(name)

    def drbd_status(resource=None):
        """Get status of specified or all DRBD replicated resources"""
        return drbd_overview_parser(resource=resource)

    def drbd_find_free_minor(self):
        return drbd_find_free_minor()

    """
    Peer Probe
    """

    def peer_get_cluster_iface(self):
        """Gets Cluster IP"""
        if 'cluster_iface' not in conf.config:
            conf.config['cluster_iface'] = 'eth1'
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
        #ip = ActivePassiveIP.objects.get(name=name)
        #return ip.is_active


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
                       registrar=rpyc.utils.registry.UDPRegistryClient(ip=cluster_iface_bcast, logger=logger),
                       auto_register=True, logger=logger, protocol_config=conf.rpyc_conn_config)
    t.start()


if __name__ == '__main__':
    main()
