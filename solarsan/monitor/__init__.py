
#from solarsan.core import logger
from setproctitle import setproctitle
from circuits import Component, Event, Debugger, handler
from .discovery import Discovery
from .peer import PeerManager
from .resource import ResourceManager
from .target import TargetManager
from .device import DeviceManager
from .ha import FloatingIPManager
from .auto_snapshot import AutoSnapshotManager


"""
Monitor
"""


class MonitorStatusUpdate(Event):
    """Monitor Status Update"""


class Monitor(Component):
    def __init__(self):
        super(Monitor, self).__init__()

        self.fire(MonitorStatusUpdate('Starting'))

        DeviceManager().register(self)
        FloatingIPManager().register(self)
        PeerManager().register(self)
        Discovery().register(self)
        TargetManager().register(self)
        ResourceManager().register(self)
        AutoSnapshotManager().register(self)

    def started(self, *args):
        self.fire(MonitorStatusUpdate('Started'))
        return True

    @handler('monitor_status_update', channel='*')
    def status_update(self, append=None):
        title = 'SolarSan Monitor'
        if append:
            title += ': %s' % append
        setproctitle('[%s]' % title)


def main():
    try:
        #(Monitor() + Debugger()).run()
        (Monitor()).run()
    except (SystemExit, KeyboardInterrupt):
        raise


if __name__ == '__main__':
    main()
