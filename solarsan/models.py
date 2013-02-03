
from datetime import datetime
import mongoengine as m


"""
Document MixIns
"""


class CreatedModifiedDocMixIn(object):
    meta = {'abstract': True}
    created = m.DateTimeField(default=datetime.utcnow())
    modified = m.DateTimeField(default=datetime.utcnow())

    def save(self, *args, **kwargs):
        """Overrides save for created and modified properties"""
        if not self.pk:
            self.created = datetime.utcnow()
        if self._changed_fields:
            self.modified = datetime.utcnow()
        return super(CreatedModifiedDocMixIn, self).save(*args, **kwargs)


"""
Config
"""


class Config(CreatedModifiedDocMixIn, m.DynamicDocument):
    name = m.StringField(unique=True)

    def __repr__(self):
        append = ''
        if self.name:
            append += " name='%s'" % self.name
        return '<%s%s>' % (self.__class__.__name__, append)
