
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Debugger, Timer
from .discovery import Discovery
from .peer import PeerManager
from .resource import ResourceManager
from .target import TargetManager
from .device import DeviceManager
from .ha import FloatingIPManager
#from .auto_snapshot import AutoSnapshotManager
from .logs import LogWatchManager
from .dkv import DkvManager
from .base import set_proc_status


"""
Monitor
"""


class ManagersCheck(Event):
    """Manager check"""


class MonitorStarted(Event):
    """Monitor started"""


class Monitor(Component):
    debug = True
    check_every = 300.0

    def __init__(self):
        set_proc_status('Init')
        super(Monitor, self).__init__()
        if self.debug:
            #Debugger(logger=logger, prefix="\ndebugger").register(self)
            Debugger(logger=logger).register(self)

        #PeerManager().register(self)
        #Discovery().register(self)
        DeviceManager().register(self)
        #FloatingIPManager().register(self)
        #ResourceManager().register(self)
        #TargetManager().register(self)
        # TODO Finish this
        #AutoSnapshotManager().register(self)
        # TODO Finish this
        #BackupManager().register(self)
        #LogWatchManager().register(self)
        DkvManager().register(self)

        self._check_timer = Timer(self.check_every,
                                  ManagersCheck(),
                                  #self.channel,
                                  persist=True,
                                  ).register(self)

    def started(self, *args):
        set_proc_status('Starting')
        self.fire(ManagersCheck())
        self.fire(MonitorStarted())

    def monitor_started(self):
        set_proc_status()


def main():
    set_proc_status('__main__')

    try:
        (Monitor()).run()
    except (SystemExit, KeyboardInterrupt):
        raise


if __name__ == '__main__':
    main()
