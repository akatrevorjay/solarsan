
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
        self.fip = FloatingIP.objects.get(name=name)
        super(FloatingIpNode, self).__init__()

    def ui_children_factory_interfaces_list(self):
        return self.fip.interfaces

    def ui_children_factory_interfaces(self, name):
        return FloatingIpInterfaceNode(self.fip, name)

    def summary(self):
        if self.fip.is_active:
            return ('active', True)
        else:
            return ('inactive', False)

    def ui_command_is_active(self):
        return self.fip.is_active

    def ui_command_add_interface(self, name, address):
        name = self.fip.clean_iface_name(name)
        self.fip.add_interface(name, address)
        return 'Added interface %s' % name

    def ui_command_remove_interface(self, name):
        name = self.fip.clean_iface_name(name)
        self.fip.remove_interface(name)
        return 'Removed interface %s' % name


from solarsan.configure.cli import InterfaceNode


class FloatingIpInterfaceNode(InterfaceNode):
    def __init__(self, fip, name):
        self.fip = fip
        super(FloatingIpInterfaceNode, self).__init__(name)
