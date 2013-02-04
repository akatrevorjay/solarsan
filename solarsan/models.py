
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
        if self._get_changed_fields():
            self.modified = datetime.utcnow()
        return super(CreatedModifiedDocMixIn, self).save(*args, **kwargs)


class ReprMixIn(object):
    def __repr__(self):
        append = ''

        repr_vars = getattr(self, '_repr_vars', ['name'])
        for k in repr_vars:
            v = getattr(self, k, None)
            if v:
                try:
                    append += " %s='%s'" % (k, v)
                except:
                    pass

        return '<%s%s>' % (self.__class__.__name__, append)


"""
Config
"""


class Config(CreatedModifiedDocMixIn, ReprMixIn, m.DynamicDocument):
    name = m.StringField(unique=True)
