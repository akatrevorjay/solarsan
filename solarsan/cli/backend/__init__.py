
from .base import AutomagicNode


"""
Main
"""


def main():
    from solarsan.core import logger
    from solarsan.cluster.models import Peer
    from solarsan.conf import rpyc_conn_config
    from rpyc.utils.server import ThreadedServer
    from setproctitle import setproctitle
    from .service import CLIService
    import rpyc

    title = 'SolarSan CLI'
    setproctitle('[%s]' % title)

    local = Peer.get_local()
    cluster_iface_bcast = local.cluster_nic.broadcast
    # Allow all public attrs, because exposed_ is stupid and should be a
    # fucking decorator.
    t = ThreadedServer(CLIService, port=18863,
                       registrar=rpyc.utils.registry.UDPRegistryClient(ip=cluster_iface_bcast,
                                                                       #logger=None,
                                                                       logger=logger,
                                                                       ),
                       auto_register=True,
                       logger=logger,
                       #logger=None,
                       protocol_config=rpyc_conn_config)
    t.start()


if __name__ == '__main__':
    main()
