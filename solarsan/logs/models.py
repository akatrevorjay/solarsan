
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.models import ReprMixIn
import mongoengine as m
import iso8601


class Syslog(ReprMixIn, m.Document):
    facility = m.StringField()
    host = m.StringField()
    host_from = m.StringField()
    isodate = m.StringField()
    message = m.StringField()
    pid = m.StringField()
    priority = m.StringField()
    program = m.StringField()
    seqnum = m.StringField()
    source = m.StringField()
    sourceip = m.StringField()
    tags = m.StringField()
    unixtime = m.StringField()

    _repr_vars = ['date', 'host', 'program', 'pid', 'priority']
    meta = {'collection': 'syslog',
            'max_size': 1024 * 1024 * 256,
            #'ordering': ['-unixtime'],
            'ordering': ['pk'],
            'indexes': ['pk', '-pk', 'program', 'pid', 'isodate', 'unixtime', 'priority'],
            'allow_inheritance': False,
            }

    def __str__(self):
        return '{0.date} {0.host} {0.program}[{0.pid}]: [{0.priority}] {0.message}'.format(self)

    def __unicode__(self):
        return unicode(self.__str__())

    @property
    def date(self):
        if not getattr(self, 'isodate', None):
            return
        return iso8601.parse_date(object.__getattribute__(self, 'isodate'))
