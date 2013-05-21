
from ..encoders import pipeline


class DkvEvent(object):
    msg = None  # DkvMsg() object
    event = None

    # Events
    CREATE = 0
    UPDATE = 1
    DELETE = 2

    def __init__(self, data=None):
        if data:
            self.__dict__ = data.copy()
            del data

    def dump(self):
        data = self.__dict__
        return pipeline.dump(data)

    def load(cls, data):
        data = pipeline.loads(data)
        return cls(**pipeline.load(data))


class DkvUpdate(DkvEvent):
    event = DkvEvent.UPDATE


class DkvDelete(DkvEvent):
    event = DkvEvent.DELETE


class DkvCreate(DkvEvent):
    event = DkvEvent.CREATE
