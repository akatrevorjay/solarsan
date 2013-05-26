
from solarsan import logging, conf, LogMeta
logger = logging.getLogger(__name__)


import machine


class MyMachine(machine.Machine):
    def initial(self):
        while True:
            ev = yield
            if ev.type == True:
                self.transition("and_now", ev.key)

    def and_now(self, key):
        print "you pressed the %s key" % key
        while True:
            ev = yield
            if self.duration() > 5.0:
                self.transition("COUNTDOWN")

    def and_then(self):
        i = 10
        while True:
            ev = yield
            print "i = %d" % i
            if i == 0:
                self.transition("IDLE")
            i -= 1


from reflex.base import Reactor
from reflex.data import Event, Binding
from reflex.control import EventManager, Ruleset, \
    ReactorBattery, RulesetBattery, PackageBattery


class TestEvent(machine.Event):
    type = True


class Plugin(Reactor):
    __metaclass__ = LogMeta

    def init(self, *args, **kwargs):
        self.name = 'Example Reactor'
        self.bind(self.evt_handler, 'example')
        #self.bind(self.my_handler, 'data_received', ['google.com'])
        self.bind(self.my_handler, 'data_received')

    def evt_handler(self, event, *args, **kwargs):
        print('>> handling event "{0}"'.format(event.name))

    def my_handler(self, event, *args, **kwargs):
        self.log.debug('Handling event %s args=%s kwargs=%s', event, args, kwargs)


events = EventManager()

Plugin(events)

events.trigger(Event('example'))

# Create a new Event object
evt = Event('data_received', [(
    'url', 'google.com'), ('data', 'insert html here')])
# evt is now an object representing the event 'data_received'. The object
# has the following attributes:
#
#   evt.name = 'data_received'
#   evt.rules = ['google.com', 'insert html here']
#   evt.url = 'google.com'
#   evt.data = 'insert html here'
#
# This data can be used in an application easily.


domain = 'google.com'
data = 'whoa'

events.trigger(Event('data_received',
                     [('address', domain), ('data', data)]))
