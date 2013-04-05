
from solarsan.exceptions import SolarSanError
from solarsan.cli.backend import AutomagicNode
from solarsan.cluster.cli import PeersNode
from solarsan.ha.cli import FloatingIpsNode

from .models import Nic
from .config import write_network_interfaces_config


class NetworkingNode(AutomagicNode):
    def ui_child_peers(self):
        return PeersNode()

    def ui_child_floating_ips(self):
        return FloatingIpsNode()

    def ui_child_interfaces(self):
        return NetworkInterfaces()


class NetworkInterfaces(AutomagicNode):
    def ui_children_factory_interface_list(self):
        # TODO Don't just remove all ifaces with : in it.
        return [iface for iface in Nic.list().keys() if ':' not in iface]

    def ui_children_factory_interface(self, name):
        return InterfaceNode(name)

    def ui_command_write_interfaces(self):
        write_network_interfaces_config(confirm=True)
        return True


class InterfaceNode(AutomagicNode):
    def __init__(self, name):
        self.obj = Nic(name)
        super(InterfaceNode, self).__init__()

        self.define_config_group_param('interface', 'proto', 'string', 'dhcp|static')
        self.define_config_group_param('interface', 'ipaddr', 'string', 'IP Address')
        self.define_config_group_param('interface', 'netmask', 'string', 'Network mask')

    def summary(self):
        if self.obj.config and self.obj.config.proto:
            return ('proto=%s' % self.obj.config.proto, True)
        elif self.obj.name == 'lo':
            return ('lo', True)
        else:
            return ('Unconfigured', False)

    #def list_config(self):
    #    pass

    def ui_getgroup_interface(self, config):
        '''
        This is the backend method for getting configs.
        @param config: The config to get the value of.
        @type config: str
        @return: The config's value
        @rtype: arbitrary
        '''
        return self.configfunc(config, None)

    def ui_setgroup_interface(self, config, value):
        '''
        This is the backend method for setting configs.
        @param config: The config to set the value of.
        @type config: str
        @param value: The config's value
        @type value: arbitrary
        '''
        return self.configfunc(config, value)

    def configfunc(self, k, v):
        if not k in self.obj.config._fields.keys():
            raise KeyError
        if v is None:
            return getattr(self.obj.config, k)
        setattr(self.obj.config, k, v)
        self.obj.config.save()
        return v

    def ui_command_apply(self, confirm=False):
        if not confirm:
            raise SolarSanError("You must specify confirm=True")

        # TODO Write interfaces, then ifdown interface, then ifup it.

        raise NotImplemented
