
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer


"""
Log Manager
"""


class LogManager(Component):
    #check_every = 300.0

    def __init__(self):
        super(LogManager, self).__init__()
        self.monitors = {}

        self.logs_check()

        #self._check_timer = Timer(self.check_every,
        #                          FloatingIpsCheck(),
        #                          persist=True,
        #                          ).register(self)

    def logs_check(self):
        pass
        #uuids = []
        #for fip in FloatingIP.objects.all():
        #    self.add_log(fip)
        #    uuids.append(fip.uuid)
        #for uuid in self.monitors.keys():
        #    if uuid not in uuids:
        #        self.monitors[uuid].unregister()
        #        self.monitors.pop(uuid)

    def add_log(self, filename):
        pass
        #if fip.uuid in self.monitors:
        #    return
        #self.monitors[fip.uuid] = FloatingIPMonitor(fip.uuid).register(self)


"""
Log Watcher
"""


class LogWatcher(Component):
    def __init__(self, filename):
        super(LogWatcher, self).__init__()


"""
Mongo Log Watcher
"""


class MongoLogWatcher(Component):
    def __init__(self, db, collection):
        super(MongoLogWatcher, self).__init__()
