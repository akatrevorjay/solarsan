
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.models import ReprMixIn
import mongoengine as m


class Syslog(ReprMixIn, m.DynamicDocument):
    _repr_vars = ['DATE', 'MESSAGE']
    meta = {'collection': 'syslog',
            'max_size': 1024 * 1024 * 256,
            'ordering': ['-DATE'],
            'indexes': ['-DATE', 'PRIORITY'],
            'allow_inheritance': False,
            }

    #def __getattribute__(self, key):
    #    if key.isupper():
    #        key = key.lower()
    #    return super(Syslog, self).__getattribute__(key)

    #def __setattribute__(self, key, value):
    #    if key.isupper():
    #        key = key.lower()
    #    return super(Syslog, self).__setattribute__(key, value)
