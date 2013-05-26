# Copyright (c) 2009 Eric Gradman (Monkeys & Robots)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


"""
pystates - A simple and powerful python state machine framework using coroutines

Example:

  from pystates import StateMachine, State

  class MyMachine(StateMachine):
    def IDLE(self):
      while True:
        ev = yield
        if ev.type == pygame.KEYDOWN:
          self.transition("RUNNING", ev.key)

    class RUNNING(State):
      def eval(self, key):
        print "you pressed the %s key" % key
        while True:
          ev = yield
          if self.duration() > 5.0:
            self.transition("COUNTDOWN")

    class COUNTDOWN(State):
      def eval(self):
        i = 10
        while True:
          ev = yield
          print "i = %d" % i
          if i == 0:
            self.transition("IDLE")
          i -= 1

See the README for a details on how to implement your own StateMachines
"""


from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.exceptions import SolarSanError

import sys
import inspect
import time


def _get_caller_module_name():
    def inner():
        return sys._getframe(2)
    f = None
    m = None
    ret = None
    try:
        f = inner()
        m = inspect.getmodule(f)
        ret = m.__name__
    except:
        pass
    finally:
        del f
        del m
    return ret


class LogMeta(type):

    def __call__(cls, *args, **kwargs):
        if 'log' not in kwargs:
            kwargs['log'] = logging.getLogger('%s.%s' % (_get_caller_module_name(), cls.__name__))
        return type.__call__(cls, *args, **kwargs)


class LogMixin:
    __metaclass__ = LogMeta


#class _FakeLog(object):
#
#    def debug(self):
#        pass
#
#_fake_log = _FakeLog()
#log = _fake_log


class MachineError(SolarSanError):

    """Generic Machine Error"""


class StateDoesNotExist(MachineError):

    """State does not exist"""


class Event:
    type = None
    #transition = None
    #condition = None
    #condition = lambda self, machine: True


class Machine(object):

    """StateMachine
    Do not instantiate this class directly.  Instead, make a subclass.  Your
    subclass should contain nested subclasses of State that implement the states
    your machine can achieve.
    """

    __metaclass__ = LogMeta
    initial_state = 'initial'

    def __init__(self, name=None, time=time.time, log=None):
        """
        Keyword arguments:
          name: The name by which your StateMachine is known. It defaults to the name of
                the class
          time: An alternative function used to tell time.  For example, sometimes with
                pygame its useful to use pygame.ticks for consistency.  It defaults to
                time.time()
          log:  If you supply a python logging object, your StateMachine will use it to
                log transitions.
        """
        self.name = name and name or str(self.__class__.__name__)
        self.time = time

        if log:
            self.log = log

        initial = getattr(self, self.initial_state, None)
        if initial:
            self.start(initial)

    #def initial(self):
    #    pass

    @property
    def current(self):
        return self.state_gen.__name__

    @property
    def current_state(self):
        return getattr(self, self.current)

    def handle(self, ev):
        """
        When you call this method, this machine's current state will resume with
        the supplied ev object.
        """
        try:
            return self.state_gen.send(ev)
        except StopIteration as exc:
            self.state_gen = exc.args[0]

    def __call__(self, *args, **kwargs):
        if args:
            try:
                state = self.get_state(args[0])
                self.activate_state(state, args)
                return
            except StateDoesNotExist:
                pass
        self.handle(*args, **kwargs)


    def start(self, state_func, *args, **kwargs):
        """
        If this machine has a state named by the state_func argument, then the machine
        will activate the named state.  This is essentially a transition from a NULL
        state to the named state.

        Any args are passed to the eval method of the named state.
        """
        self.state_gen = self.activate_state(state_func, *args, **kwargs)

    def transition(self, state_func, *args, **kwargs):
        """
        If this machine has a state named by the state_func argument, then the machine
        will transition to the named state.

        Any args are passed to the new state_func.
        """
        state_gen = self.activate_state(state_func, *args, **kwargs)
        raise StopIteration(state_gen)

    def get_state(self, state_func):
        if isinstance(state_func, basestring):
            name = state_func
        else:
            name = state_func.__name__
        state_func = getattr(self, name, None)
        if not state_func:
            raise StateDoesNotExist
        return name, state_func

    def activate_state(self, state_func, *args, **kwargs):
        name, state_func = self.get_state(state_func)
        self.log.debug("%s activating state %s", str(self), name)

        self.state_start_time = self.time()
        state_gen = state_func(*args, **kwargs)
        # state_gen.next()
        return state_gen

    def duration(self):
        return self.time() - self.state_start_time

    def __str__(self):
        return "<Machine:%s>" % self.name
