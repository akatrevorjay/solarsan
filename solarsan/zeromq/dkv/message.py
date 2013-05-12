
from solarsan import logging, signals, conf
logger = logging.getLogger(__name__)
from solarsan.exceptions import SolarSanError
from solarsan.pretty import pp
import threading
import time
from datetime import datetime

#from .beacon_greeter import GreeterBeacon
from ..encoders import pipeline
from ..utils import get_address, parse_address
from ..zhelpers import zpipe
import zmq



class Message(dict):
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
        return '<%s "%s">' % (self.__class__.__name__, dict.__repr__(self))

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
