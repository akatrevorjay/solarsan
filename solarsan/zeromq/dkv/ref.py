
from reflex.base import Reactor
from reflex.data import Event, Binding
from reflex.control import EventManager, Ruleset, \
    ReactorBattery, RulesetBattery, PackageBattery



class Plugin(Reactor):

    def init(self, *args, **kwargs):
        self.name = 'Example Reactor'
        self.bind(self.evt_handler, 'example')

    def evt_handler(self, event, *args, **kwargs):
        print('>> handling event "{0}"'.format(event.name))



class EM(EventManager):
    pass


events = EM()

Plugin(events)

events.trigger(Event('example'))





# Create a new Event object
evt = Event('data_received', [('url', 'google.com'), ('data', 'insert html here')])
# evt is now an object representing the event 'data_received'. The object
# has the following attributes:
#
#   evt.name = 'data_received'
#   evt.rules = ['google.com', 'insert html here']
#   evt.url = 'google.com'
#   evt.data = 'insert html here'
#
# This data can be used in an application easily.




self.bind(self.my_handler, 'data_received', ['google.com'])

manager.trigger(Event('data_received',
        [('address', domain), ('data', data)]))




