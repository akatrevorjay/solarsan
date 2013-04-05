
#from solarsan.core import logger
from solarsan import conf
import sh

from solarsan.cli.backend import AutomagicNode
from solarsan.logs.cli import LogsNode
from solarsan.alerts.cli import AlertsNode
from solarsan.developer.cli import Developer
from solarsan.configure.cli import NetworkingNode


"""
System
"""


class SystemNode(AutomagicNode):
    def __init__(self, *args, **kwargs):
        self.define_config_group_param('system', 'hostname', 'string', 'Hostname (short)')
        self.define_config_group_param('system', 'domain', 'string', 'Domain name')
        self.define_config_group_param('system', 'gateway', 'string', 'Gateway')
        self.define_config_group_param('system', 'nameservers', 'string', 'DNS resolvers')

        super(SystemNode, self).__init__(*args, **kwargs)

    def ui_getgroup_system(self, config):
        '''
        This is the backend method for getting configs.
        @param config: The config to get the value of.
        @type config: str
        @return: The config's value
        @rtype: arbitrary
        '''
        #return conf.config.get(config)
        return None

    def ui_setgroup_system(self, config, value):
        '''
        This is the backend method for setting configs.
        @param config: The config to set the value of.
        @type config: str
        @param value: The config's value
        @type value: arbitrary
        '''
        #conf.config[config] = value
        #return conf.config.save()
        return None

    def ui_child_networking(self):
        return NetworkingNode()

    def ui_child_logs(self):
        return LogsNode()

    def ui_child_alerts(self):
        return AlertsNode()

    def ui_child_developer(self):
        if conf.config.get('debug') is True:
            return Developer()

    def ui_command_hostname(self):
        '''
        Displays the system hostname
        '''
        return sh.hostname('-f')

    def ui_command_uname(self):
        '''
        Displays the system uname information.
        '''
        return sh.uname('-a')

    def ui_command_lsmod(self):
        '''
        lsmod - program to show the status of modules in the Linux Kernel
        '''
        return sh.lsmod()

    def ui_command_lspci(self):
        '''
        lspci - list all PCI devices
        '''
        return sh.lspci()

    def ui_command_lsusb(self):
        '''
        lsusb - list USB devices
        '''
        return sh.lsusb()

    def ui_command_lscpu(self):
        '''
        lscpu - CPU architecture information helper
        '''
        return sh.lscpu()

    def ui_command_lshw(self):
        '''
        lshw - List all hardware known by HAL
        '''
        return sh.lshw()

    def ui_command_uptime(self):
        '''
        uptime - Tell how long the system has been running.
        '''
        return sh.uptime()

    def ui_command_shutdown(self):
        '''
        shutdown - Shutdown system
        '''
        #status.tasks.shutdown.delay()
        return sh.shutdown('-h', 'now')

    def ui_command_reboot(self):
        '''
        reboot - reboot system
        '''
        #status.tasks.reboot.delay()
        return sh.reboot()

    def ui_command_check_services(self):
        return sh.egrep(sh.initctl('list'), 'solarsan|targetcli|mongo')
