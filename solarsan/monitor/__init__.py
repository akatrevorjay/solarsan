
from solarsan import logging, conf
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
from .dkv import DkvManager, DkvWaitForConnected
from .base import set_proc_status


"""
Monitor
"""


class ManagersCheck(Event):
    """Manager check"""


class MonitorStarted(Event):
    """Monitor started"""


class Monitor(Component):
    debug = None
    check_every = 300.0

    def __init__(self):
        if self.debug is None:
            self.debug = conf.config['debug']
        set_proc_status('Init')
        Component.__init__(self)

        if self.debug:
            Debugger(
                logger=logger.getChild('events'),
                IgnoreChannels=('discovery', 'peer', 'log_watch'),
                IgnoreEvents=('registered', 'resource_health_check', 'managers_check', 'target_check_luns')).register(self)
            #Debugger(logger=logger).register(self)

        self.dkv = DkvManager().register(self)
        self.dkv.dkv.wait_for_connected()
        #self.fire(DkvWaitForConnected())

        Inner().register(self)

    def started(self, component):
        self._check_timer = Timer(self.check_every,
                                  ManagersCheck(),
                                  #self.channel,
                                  persist=True,
                                  ).register(self)

        set_proc_status('Starting')
        self.fire(ManagersCheck())
        self.fire(MonitorStarted())

    def monitor_started(self):
        set_proc_status()


class Inner(Component):
    def __init__(self):
        Component.__init__(self)

        PeerManager().register(self)
        Discovery().register(self)
        DeviceManager().register(self)
        FloatingIPManager().register(self)
        ResourceManager().register(self)
        TargetManager().register(self)
        # TODO Finish this
        #AutoSnapshotManager().register(self)
        # TODO Finish this
        #BackupManager().register(self)
        LogWatchManager().register(self)


def main():
    set_proc_status('__main__')

    try:
        (Monitor()).run()
    except (SystemExit, KeyboardInterrupt):
        raise


if __name__ == '__main__':
    main()
