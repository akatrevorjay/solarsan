
from solarsan import logging, signals
logger = logging.getLogger(__name__)
from .models import Syslog
from . import policies
from time import time
import mongoengine as m


class MongoLogWatcher(object):
    class signals:
        check_log_entry = signals.check_log_entry

    _debug = False

    def __init__(self):
        super(MongoLogWatcher, self).__init__()
        self._last_ts = time()

    def run_once(self):
        logs = self._next()
        count = logs.count()
        if count == 0:
            return
        if self._debug:
            logger.debug('Found %s monlogs to eat.', count)
        for log in logs:
            if self._debug:
                logger.debug('Checking monlog: %s', log)
            self._check(log)

    def _next(self):
        try:
            logs = Syslog.objects.filter(unixtime__gt=str(self._last_ts))
        except m.document.InvalidCollectionError as e:
            logger.error("Monlog collection is invalid, ie is not capped. Dropping existing collection to re-initialize as such: %s", e)
            Syslog.drop_collection()
            logs = Syslog.objects.filter(unixtime__gt=str(self._last_ts))
        self._last_ts = time()
        return logs

    def _check(self, log):
        try:
            signals.check_log_entry.send(None, log=log)
            return True
        except Exception as e:
            logger.exception('Exception occurred while checking log entry "%s": %s', log, e.message)
            return False
