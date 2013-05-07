
from solarsan import logging, signals, conf
logger = logging.getLogger(__name__)
from solarsan.exceptions import SolarSanError
from solarsan.pretty import pp
import threading
import time

from .beacon import Beacon
#from .beacon_greeter import GreeterBeacon
from .serializers import pipeline
from .utils import get_address, parse_address

import zmq
from zhelpers import zpipe
from kvmsg import KVMsg

from datetime import datetime


class DkvMsg(dict):
    _sequence = None
    _pending = None
    _sent = None

    def __init__(self, **kwargs):
        dict.__init__(self, **kwargs)
        self.update(**kwargs)
        if not self.get('created_at'):
            self.created_at = datetime.now()

    def send(self):
        self.pending = True
        self.sent_at = datetime.now()

    def save(self):
        pass

    def __repr__(self):
        return '<DkvMsg "%s">' % dict.__repr__(self)

    def __setattr__(self, k, v):
        if k.startswith('_'):
            return object.__setattr__(self, k, v)
        return dict.__setitem__(self, k, v)

    def __getattr__(self, k):
        if k.startswith('_'):
            return object.__getattribute__(self, k)
        try:
            return dict.__getitem__(self, k)
        except KeyError:
            raise AttributeError


class DkvContainer(list):
    pass


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
