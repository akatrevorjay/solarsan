
from solarsan import logging
logger = logging.getLogger(__name__)
from circuits import Component, Event, Timer
from solarsan.logs.watch import MongoLogWatcher


"""
Log Manager
"""


class LogWatchRun(Event):
    """ Checks any logs if they need parsed """


class LogWatchManager(Component):
    channel = 'log_watch'

    #run_every = 300.0
    run_every = 10.0

    def __init__(self, channel=channel):
        super(LogWatchManager, self).__init__(channel=channel)
        self.monlog = MongoLogWatcher()

        self._run_timer = Timer(self.run_every,
                                LogWatchRun(),
                                self.channel,
                                persist=True,
                                ).register(self)

    def started(self, *args, **kwargs):
        self.fire(LogWatchRun())

    def log_watch_run(self):
        self.monlog.run_once()
