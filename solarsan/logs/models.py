
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.models import ReprMixIn
import mongoengine as m
import iso8601


class Syslog(ReprMixIn, m.DynamicDocument):
    _repr_vars = ['date', 'host', 'program', 'pid', 'priority']
    meta = {'collection': 'syslog',
            'max_size': 1024 * 1024 * 256,
            'ordering': ['-date'],
            'indexes': ['id', 'program', 'pid', 'date', 'unixtime', 'priority'],
            'allow_inheritance': False,
            }

    def __str__(self):
        return '%s %s %s[%s]: [%s] %s' % (self.date, self.host, self.program,
                                          self.pid, self.priority, self.message)

    def __unicode__(self):
        return unicode(self.__str__())

    @property
    def date(self):
        if not getattr(self, 'isodate', None):
            return
        return iso8601.parse_date(object.__getattribute__(self, 'isodate'))
