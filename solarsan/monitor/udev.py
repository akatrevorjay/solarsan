"""UDev Notification Support

A Component wrapping the udev API using the pyudev library
"""

try:
    import pyudev
except ImportError:
    raise Exception("No pyudev support available. Is pyudev installed?")

from circuits.core import Event, Component

context = pyudev.Context()


class DeviceAdd(Event):
    """Device Add"""


class DeviceRemove(Event):
    """Device Remove"""


class UDev(Component):

    channel = "udev"

    def __init__(self, channel=channel):
        super(UDev, self).__init__(channel=channel)

        self._monitor = pyudev.Monitor.from_netlink(context)
        self._observer = pyudev.MonitorObserver(self._monitor, self._process)

    def started(self, *args):
        self._observer.start()

    def _process(self, action, device):
        if action == 'add':
            self.fire(DeviceAdd(device))
        elif action == 'remove':
            self.fire(DeviceRemove(device))
