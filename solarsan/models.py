
import mongoengine as m


"""
Config
"""


class Config(m.DynamicDocument):
    name = m.StringField(unique=True)
    #created = m.DateTimeField()
    #modified = m.DateTimeField()
