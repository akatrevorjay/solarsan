
from solarsan.cli.backend import AutomagicNode
from .models import FloatingIP


class FloatingIpsNode(AutomagicNode):
    def ui_children_factory_floating_ip_list(self):
        return [ip.name for ip in FloatingIP.objects.all()]

    def ui_children_factory_floating_ip(self, name):
        return FloatingIpNode(name)

    def ui_command_create(self, name=None):
        # TODO Create Floating IP wizard
        raise NotImplemented


class FloatingIpNode(AutomagicNode):
    def __init__(self, name):
        self.obj = FloatingIP.objects.get(name=name)
        super(FloatingIpNode, self).__init__()

    def summary(self):
        if self.obj.is_active:
            return ('Active', True)
        else:
            return ('Inactive', False)
