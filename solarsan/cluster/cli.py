
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.cli.backend import AutomagicNode
from .models import Peer


"""
Cluster
"""


#class ClusterNode(AutomagicNode):
#    def ui_child_peers(self):
#        return PeersNode()
#
#    def ui_child_floating_ips(self):
#        return FloatingIpsNode()
#
#    def ui_child_resources(self):
#        return ResourcesNode()
#
#    # TODO Attribute for config settings such as cluster_iface


class PeersNode(AutomagicNode):
    def ui_children_factory_peer_list(self):
        return [p.hostname for p in Peer.objects.all()]

    def ui_children_factory_peer(self, name):
        #logger.debug('PeerNode factory name=%s', name)
        # TODO WHY IS THIS LOWER CASED?
        return PeerNode(name)


class PeerNode(AutomagicNode):
    def __init__(self, hostname):
        #logger.debug('PeerNode init hostname=%s', hostname)
        self.obj = Peer.objects.get(hostname__iexact=hostname)
        super(PeerNode, self).__init__()

    def summary(self):
        if self.obj.is_online:
            return ('online', True)
        else:
            return ('offline', False)

    @property
    def service(self):
        return self.obj.get_service('storage')

    def ui_command_ping(self):
        return self.service.root.ping()

    def ui_command_is_local(self):
        return self.obj.is_local
