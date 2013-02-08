
from .base import BaseServiceConfigNode


class System(BaseServiceConfigNode):
    def __init__(self, parent):
        super(System, self).__init__(None, parent)

    def ui_command_hostname(self):
        '''
        Displays the system hostname
        '''
        self()

    def ui_command_uname(self):
        '''
        Displays the system uname information.
        '''
        self()

    def ui_command_lsmod(self):
        '''
        lsmod - program to show the status of modules in the Linux Kernel
        '''
        self()

    def ui_command_lspci(self):
        '''
        lspci - list all PCI devices
        '''
        self()

    def ui_command_lsusb(self):
        '''
        lsusb - list USB devices
        '''
        self()

    def ui_command_lscpu(self):
        '''
        lscpu - CPU architecture information helper
        '''
        self()

    def ui_command_lshw(self):
        '''
        lshw - List all hardware known by HAL
        '''
        self()

    def ui_command_uptime(self):
        '''
        uptime - Tell how long the system has been running.
        '''
        self()

    def ui_command_shutdown(self):
        '''
        shutdown - Shutdown system
        '''
        self()

    def ui_command_reboot(self):
        '''
        reboot - reboot system
        '''
        self()

    def ui_command_check_services(self):
        self()
