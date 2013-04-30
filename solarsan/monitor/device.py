
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component
from solarsan.monitor.udev import UDev


"""
Device Manager
"""


class DeviceManager(Component):
    channel = 'device'

    def __init__(self, channel=channel):
        super(DeviceManager, self).__init__(channel=channel)

        UDev(channel=channel).register(self)

    def device_add(self, device):
        logger.debug('Device added: %s', device)

    def device_remove(self, device):
        logger.debug('Device removed: %s', device)
