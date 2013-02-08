
from solarsan.core import logger
from solarsan import conf
from solarsan.cluster.models import Peer
from configshell import ConfigNode

import os


class System(ConfigNode):
    def __init__(self, parent):
        super(System, self).__init__('system', parent)
        self._service = None

    @property
    def service(self):
        if self._service:
            p = Peer.get_local()
            self._service_cli = p.get_service('cli')
            self._service = self._service_cli.root.system()
        return self._service

    def ui_command_hostname(self):
        '''
        Displays the system hostname
        '''
        self._service.hostname()

    def ui_command_uname(self):
        '''
        Displays the system uname information.
        '''
        os.system("uname -a")

    def ui_command_lsmod(self):
        '''
        lsmod - program to show the status of modules in the Linux Kernel
        '''
        os.system("lsmod")

    def ui_command_lspci(self):
        '''
        lspci - list all PCI devices
        '''
        os.system("lspci")

    def ui_command_lsusb(self):
        '''
        lsusb - list USB devices
        '''
        os.system("lsusb")

    def ui_command_lscpu(self):
        '''
        lscpu - CPU architecture information helper
        '''
        os.system("lscpu")

    def ui_command_uptime(self):
        '''
        uptime - Tell how long the system has been running.
        '''
        os.system("uptime")

    def ui_command_shutdown(self):
        '''
        shutdown - Shutdown system
        '''
        #status.tasks.shutdown.delay()
        raise NotImplemented

    def ui_command_reboot(self):
        '''
        reboot - reboot system
        '''
        #status.tasks.reboot.delay()
        raise NotImplemented

    def ui_command_check_services(self):
        os.system("initctl list | egrep 'solarsan|targetcli|mongo'")
