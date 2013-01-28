
#from solarsan import conf
from solarsan.core import logger
from solarsan.cluster.models import Peer
import rpyc


class MyService(rpyc.Service):
    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        pass

    def exposed_get_answer(self):  # this is an exposed method
        return 42

    def get_question(self):  # while this method is not exposed
        return "what is the airspeed velocity of an unladen swallow?"


class StorageService(rpyc.Service):
    def on_connect(self):
        logger.debug('Client connected.')

    def on_disconnect(self):
        logger.debug('Client disconnected.')

    def exposed_ping(self):
        return True


def main():
    from rpyc.utils.server import ThreadedServer
    local = Peer.get_local()
    cluster_iface_bcast = local.cluster_nic.broadcast
    t = ThreadedServer(StorageService, port=18861,
                       registrar=rpyc.utils.registry.UDPRegistryClient(ip=cluster_iface_bcast, logger=logger),
                       auto_register=True, logger=logger)
    t.start()


if __name__ == '__main__':
    main()
