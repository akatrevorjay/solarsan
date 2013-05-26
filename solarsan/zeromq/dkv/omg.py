
import sys
import inspect


def omg():
    return inspect.currentframe

def omg2():
    f = omg()

    print f

    try:
        fi = inspect.getframeinfo(f)
        print fi
    except: pass


    try:
        print inspect.getmodule(f)
    except: pass
    try:
        print inspect.getmodule(fi)
    except: pass


def omg3():
    return sys._getframe(1)

def omg4():
    return omg3()


from reflex.data import Event
from reflex.control import EventManager

#Event.


import machine


class TestEvent(machine.Event):
    type = True


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
