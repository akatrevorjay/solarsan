
from solarsan.core import logger
from circuits import Component
from solarsan.monitor.udev import UDev


"""
Device Manager
"""


class DeviceManager(Component):
    def __init__(self):
        super(DeviceManager, self).__init__()

        UDev().register(self)

    def device_add(self, device):
        logger.debug('Device added: %s', device)

    def device_remove(self, device):
        logger.debug('Device removed: %s', device)
