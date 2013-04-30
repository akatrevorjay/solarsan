
from solarsan import logging, signals
logger = logging.getLogger(__name__)
from .models import Syslog
from . import policies
import mongoengine as m


class BaseMongoLogWatcher(object):
    _debug = False

    def __init__(self, last=0):
        self._last_count = last

        if last == 0:
            self._last = Syslog.objects.order_by('-pk')[last]
        else:
            for x in xrange(last):
                x = last - (x + 1)
                try:
                    self._last = Syslog.objects.order_by('-pk')[last]
                    break
                except Syslog.DoesNotExist:
                    pass

    def next_chunk(self):
        try:
            ret = Syslog.objects.filter(pk__gt=self._last.pk)

            try:
                self._last = Syslog.objects.order_by('-pk')[0]
            except Syslog.DoesNotExist:
                return

            return ret
        except m.document.InvalidCollectionError as e:
            logger.error("Monlog collection is invalid, ie is not capped. Dropping existing collection to re-initialize as such: %s", e)
            Syslog.drop_collection()

    def __iter__(self):
        return iter(self.next_chunk())


class MongoLogWatcher(BaseMongoLogWatcher):
    class signals:
        check_log_entry = signals.check_log_entry

    def run_once(self):
        for log in self.next_chunk():
            if self._debug:
                logger.debug('Checking monlog: %s', log)
            self._check(log)

    def _check(self, log):
        try:
            signals.check_log_entry.send(None, log=log)
            return True
        except Exception as e:
            logger.exception('Exception occurred while checking log entry "%s": %s', log, e.message)
            return False
