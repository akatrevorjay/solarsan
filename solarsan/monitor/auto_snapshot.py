
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.conf import config
from circuits import Component, Event, Timer
#from solarsan.storage.pool import Pool
#from solarsan.storage.snapshot import Snapshot


"""
Auto Snapshot Manager
"""


class AutoSnapshot(Event):
    """Automatic Snapshot"""


class AutoSnapshotManager(Component):
    def __init__(self):
        super(AutoSnapshotManager, self).__init__()

        auto_snap_conf = dict(config.get('auto_snap', {}))
        if not auto_snap_conf:
            return

        self._conf = {}
        for name, conf in auto_snap_conf.iteritems():
            conf = self._conf[name] = conf
            conf['timer'] = Timer(float(conf['interval']),
                                  AutoSnapshot(name, name),
                                  persist=True).register(self)

    def auto_snapshot(self, prefix):
        logger.error('TODO Auto snapshot')
        raise NotImplemented
