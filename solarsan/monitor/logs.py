
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer
from solarsan.logs.watch import MongoLogWatcher


"""
Log Manager
"""


class LogWatchRun(Event):
    """ Checks any logs if they need parsed """


class LogManager(Component):
    #run_every = 300.0
    run_every = 10.0

    def __init__(self):
        super(LogManager, self).__init__()
        self.monlog = MongoLogWatcher()

        self._run_timer = Timer(self.run_every,
                                LogWatchRun(),
                                persist=True,
                                ).register(self)

    def started(self, *args, **kwargs):
        self.fire(LogWatchRun())

    def log_watch_run(self):
        self.monlog.run_once()
