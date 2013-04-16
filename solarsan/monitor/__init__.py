
#from solarsan.core import logger
from circuits import Component, Event, Debugger, handler
from .discovery import Discovery
from .peer import PeerManager
from .resource import ResourceManager
from .target import TargetManager
from .device import DeviceManager
from .ha import FloatingIPManager
#from .auto_snapshot import AutoSnapshotManager
from .logs import LogManager
from .base import set_proc_status


"""
Monitor
"""


class MonitorStatusUpdate(Event):
    """Monitor Status Update"""


class Monitor(Component):
    def __init__(self):
        set_proc_status('Init')
        super(Monitor, self).__init__()

        PeerManager().register(self)
        Discovery().register(self)
        DeviceManager().register(self)
        FloatingIPManager().register(self)
        TargetManager().register(self)
        ResourceManager().register(self)
        # TODO Finish this
        #AutoSnapshotManager().register(self)
        # TODO Finish this
        #BackupManager().register(self)
        LogManager().register(self)

    def started(self, *args):
        set_proc_status('Started')
        return True


def main():
    try:
        (Monitor() + Debugger()).run()
        #(Monitor()).run()
    except (SystemExit, KeyboardInterrupt):
        raise


if __name__ == '__main__':
    set_proc_status('Starting')
    main()
