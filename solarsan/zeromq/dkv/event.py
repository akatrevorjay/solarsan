
from collections import defaultdict

import traceback
import sys


class Event(list):
    """Event subscription.

    A list of callable objects. Calling an instance of this will cause a
    call to each item in the list in ascending order by index.

    Example Usage:
    >>> def f(x):
    ...     print 'f(%s)' % x
    >>> def g(x):
    ...     print 'g(%s)' % x
    >>> e = Event()
    >>> e()
    >>> e.append(f)
    >>> e(123)
    f(123)
    >>> e.remove(f)
    >>> e()
    >>> e += (f, g)
    >>> e(10)
    f(10)
    g(10)
    >>> del e[0]
    >>> e(2)
    g(2)

    """
    def __call__(self, *args, **kwargs):
        for f in self:
            f(*args, **kwargs)

    def __repr__(self):
        return "Event(%s)" % list.__repr__(self)

class Event:
    def __init__(self):
        self.handlers = set()

    def handle(self, handler):
        self.handlers.add(handler)
        return self

    def unhandle(self, handler):
        try:
            self.handlers.remove(handler)
        except:
            raise ValueError("Handler is not handling this event, so cannot unhandle it.")
        return self

    def fire(self, *args, **kargs):
        for handler in self.handlers:
            handler(*args, **kargs)

    def getHandlerCount(self):
        return len(self.handlers)

    __iadd__ = handle
    __isub__ = unhandle
    __call__ = fire
    __len__  = getHandlerCount

class MockFileWatcher:
    def __init__(self):
        self.fileChanged = Event()

    def watchFiles(self):
        source_path = "foo"
        self.fileChanged(source_path)

def log_file_change(source_path):
    print "%r changed." % (source_path,)

def log_file_change2(source_path):
    print "%r changed!" % (source_path,)

watcher              = MockFileWatcher()
watcher.fileChanged += log_file_change2
watcher.fileChanged += log_file_change
watcher.fileChanged -= log_file_change2
watcher.watchFiles()


# from customevent
class Event(object):
    def __init__(self, threaded=None, spawn=None):
        """Initialise new event object.
            connected = event(threaded=True)
        """
        if threaded:
            self.__call__ = self._call_threaded
        if spawn:
            if isinstance(spawn, (int, float)):
                self.spawn_later = spawn
            self.__call__ = self._call_spawn
        self.__handlers__ = []

    def __iadd__(self, handler):
        """Add event handler to event object.
            connected += on_connected
        """
        self.__handlers__.append(handler)
        return self

    def __isub__(self, handler):
        """Remove event handler from event object.
            connected -= on_connected
        """
        self.__handlers__.remove(handler)
        return self

    def __iand__(self, handler):
        """
            connected &= on_connected
        """
        self.__handlers__ = [handler] if handler else []
        return self

    def __ior__(self, handler):
        """
            connected |= on_connected
        """
        if handler in self.__handlers__:
            return self
        return self.__iadd__(handler)

    def __ixor__(self, handler):
        """
            connected ^= on_connected
        """
        if handler in self.__handlers__:
            self.__handlers__.remove(handler)
        return self

    def __contains__(self, handler):
        """
            on_connected in connected
        """
        return True if handler in self.__handlers__ else False

    def __repr__(self):
        return "<%s handlers='%s'>" % (self.__class__.__name__, self.__handlers__)

    def __len__(self):
        return len(self.__handlers__)

    def handler(self, func):
        """
            @connected.handler
            def on_connected(): pass
        """
        self.__ior__(func)

    on = handler

    def _call(self, *args, **kwargs):
        """
            connected()
        """
        for handler in self.__handlers__:
            try:
                handler(*args, **kwargs)
            except Exception:
                traceback.print_exc(file=sys.stderr)
                continue

    __call__ = _call

    def _call_threaded(self, *args, **kwargs):
        import threading
        threading.Thread(target=self._call, args=args, kwargs=kwargs).start()

    def _call_spawn(self, *args, **kwargs):
        import gevent
        if hasattr(self, 'spawn_later'):
            gevent.spawn_later(self.spawn_later, self._call, *args, **kwargs)
        else:
            gevent.spawn(self._call, *args, **kwargs)



class EventSink(object):
    __event_cls__ = Event
    __event__opts__ = None
    __events__ = None
    __handlers__ = None

    def __init__(self, threaded=None, spawn=None):
        """Initialise new event object.
            connected = event(threaded=True)
        """
        self.__event_opts__ = dict(threaded=threaded, spawn=spawn)
        self.__events__ = defaultdict()
        self.__sinks__ = self.__getattr__('sinks')

    def __getattr__(self, key):
        if not key.startswith('_'):
            if key not in self:
                self.__events__[key] = self.__event_cls__(**self.__event_opts__)
            return self.__events__.get(key)

    def __iadd__(self, handler):
        """Add event handler to event object.
            connected += on_connected
        """
        self.__sinks__.append(handler)
        return self

    def __isub__(self, handler):
        """Remove event handler from event object.
            connected -= on_connected
        """
        self.__sinks__.remove(handler)
        return self

    def __iand__(self, handler):
        """
            connected &= on_connected
        """
        self.__sinks__ = [handler] if handler else []
        return self

    def __ior__(self, handler):
        """
            connected |= on_connected
        """
        if handler in self:
            return self
        return self.__iadd__(handler)

    def __ixor__(self, handler):
        """
            connected ^= on_connected
        """
        if handler in self.__sinks__:
            self.__sinks__.remove(handler)
        return self

    def __contains__(self, handler):
        """
            on_connected in connected
        """
        return handler in self.__sinks__

    def __repr__(self):
        return "<%s handlers='%s'>" % (self.__class__.__name__, self.__handlers__)

    def __len__(self):
        return len(self.__handlers__)

    def __iter__(self):
        for e in self.__events__:
            yield e

    def handler(self, func, event=None):
        """
            @connected.handler
            def on_connected(): pass
        """
        if event:
            if isinstance(event, basestring):
                event = getattr(self, event)
            event.handler(func)
        else:
            self.__ior__(func)

    on = handler

    def __call__(self, event, *args, **kwargs):
        """
            connected()
        """
        if isinstance(event, basestring):
            event = getattr(self, event)
        self.__sinks__(*args, **kwargs)
        event(*args, **kwargs)


class EventManager:

    '''
    from event import Event

    class FileWatcher:
        def __init__(self):
            self.fileChanged = Event()

        def watchFiles(self):
            self.fileChanged(source_path)

    def log_file_change(source_path):
        print "%r changed." % (source_path,)

    watcher              = FileWatcher()
    watcher.fileChanged += log_file_change
    '''
    def __init__(self):
        self.handlers = set()

    def handle(self, handler):
        self.handlers.add(handler)
        return self

    def unhandle(self, handler):
        try:
            self.handlers.remove(handler)
        except:
            raise ValueError(
                "Handler is not handling this event, so cannot unhandle it.")
        return self

    def fire(self, *args, **kargs):
        for handler in self.handlers:
            handler(*args, **kargs)

    def getHandlerCount(self):
        return len(self.handlers)

    __iadd__ = handle
    __isub__ = unhandle
    __call__ = fire
    __len__ = getHandlerCount


# From thor
class EventEmitter(object):

    """
    An event emitter, in the style of Node.JS.
    """

    def __init__(self):
        self.__events = defaultdict(list)
        self.__sink = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_EventEmitter__events"]
        return state

    def on(self, event, listener):
        """
        Call listener when event is emitted.
        """
        self.__events[event].append(listener)
        self.emit('newListener', event, listener)

    def once(self, event, listener):
        """
        Call listener the first time event is emitted.
        """
        def mycall(*args):
            listener(*args)
            self.removeListener(event, mycall)
        self.on(event, mycall)

    def removeListener(self, event, listener):
        """
        Remove a specific listener from an event.

        If called for a specific listener by a previous listener
        for the same event, that listener will not be fired.
        """
        self.__events.get(event, [listener]).remove(listener)

    def removeListeners(self, *events):
        """
        Remove all listeners from an event; if no event
        is specified, remove all listeners for all events.

        If called from an event listener, other listeners
        for that event will still be fired.
        """
        if events:
            for event in events:
                self.__events[event] = []
        else:
            self.__events = defaultdict(list)

    def listeners(self, event):
        """
        Return a list of listeners for an event.
        """
        return self.__events.get(event, [])

    def events(self):
        """
        Return a list of events being listened for.
        """
        return self.__events.keys()

    def emit(self, event, *args):
        """
        Emit the event (with any given args) to
        its listeners.
        """
        events = self.__events.get(event, [])
        if len(events):
            for e in events:
                e(*args)
        else:
            sink_event = getattr(self.__sink, event, None)
            if sink_event:
                sink_event(*args)

    def sink(self, sink):
        """
        If no listeners are found for an event, call
        the method that shares the event's name (if present)
        on the event sink.
        """
        self.__sink = sink

    # TODO: event bubbling


def on(obj, event=None):
    """
    Decorator to call a function when an object emits
    the specified event.
    """
    def wrap(funk):
        obj.on(event or funk.__name__, funk)
        return funk
    return wrap



