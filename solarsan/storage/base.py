
#from solarsan.utils import LoggedException
#import logging
#import sh


class ReprMixIn(object):
    def __repr__(self):
        for k in ['name', 'guid', 'id']:
            name = getattr(self, k, None)
            if name:
                break
        return "<%s: %s>" % (self.__class__.__name__, name)

    def __unicode__(self):
        name = getattr(self, 'name', self.__repr__())
        return name


class Base(ReprMixIn):
    def __init__(self, *args, **kwargs):
        super(Base, self).__init__()

    def path(self, start=0, len=None):
        """ Splits name of object into paths starting at index start """
        return self.name.split('/')[start:len]


class BaseProperty(ReprMixIn):
    """Storage Dataset Property object
    """

    def __repr__(self):
        #return "<Property('%s'='%s', source='%s')>" % (self.name,
        #                                               self.value,
        #                                               self.source)

        prefix = ''
        source = ''
        if self.source == '-':
            prefix += 'Statistic'
        elif self.source in ['default', 'local']:
            prefix += self.source.capitalize()
        elif self.source:
            prefix += 'Inherited'
            source = ' source=%s' % self.source

        #if self.modified:
        #    prefix += 'Unsaved'

        name = prefix + self.__class__.__name__
        return "%s(%s=%s%s)" % (name, self.name, self.value, source)

    def __unicode__(self):
        return unicode(self.value)

    def __str__(self):
        return str(self.value)

    def __nonzero__(self):
        value = self.value
        if value == 'on':
            return True
        elif value:
            return False

    def __init__(self, parent, name, value, source):
        self._parent = parent
        self.name = name
        self._set(value, ignore=True)
        self.source = source

    def _get(self):
        return self._value

    def _set(self, value, ignore=False):
        if not ignore:
            self._parent._set(self.name, value)
        self._value = value

    value = property(_get, _set)
