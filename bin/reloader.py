#!/usr/bin/env python
"""Reloader
Allows to start a program, and to monitor changes in a folder, when changes are
detected in the folder, the command is restarted.

This can be useful to test a software you are developping and having immediate
feedback.
Or to restart a daemon when configuration or data changes.
Or any other use, the sky is the limit :)

Usage: reloader.py [-p <path>] [-a <action] [-a <ignorelist>] [-s <sleep>] <command>

    -p <path> --path=<path>                     Path to monitor for changes. [default: current directory]
    -a <action> --action=<action>               Action to perform when changes are detected. [default: restart]
    -i <ignorelist> --ignorelist=<ignorelist>   Files to ignore.
    -s <sleep> --sleep=<sleep>                  Ignore events for this many seconds after the last restart [default: 10]
    <command>                                   Command to run and restart

"""
import logging
log = logging.getLogger(__name__)
from docopt import docopt
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileMovedEvent, FileModifiedEvent
from subprocess import Popen
from time import time, sleep
import re


class RestartHandler(FileSystemEventHandler):
    def __init__(self, command, path, ignorelist, focus, sleeptime, **kwargs):
        super(RestartHandler, self).__init__(**kwargs)
        self.command = command
        self.ignorelist = ignorelist
        self.focus = focus
        self.sleep = sleeptime
        self.start()

    def stop(self):
        self._process.terminate()
        log.error('Terminated process')

    def start(self):
        self._last_restart = time()

        self._process = Popen(self.command)
        log.info('Started %s' % self._process)

    def on_any_event(self, event):
        #if not isinstance(event, (FileMovedEvent, FileModifiedEvent)):
        if not isinstance(event, (FileModifiedEvent, )):
            return

        if self.sleep and time() < self._last_restart + self.sleep:
            return

        for i in self.ignorelist:
            r = re.compile('^' + i.replace('*', '.*') + '$')
            if r.match(event.src_path):
                return
            if isinstance(event, FileMovedEvent) and r.match(event.dest_path):
                return

        log.warning('Restarting due to %s', event)

        self.stop()
        self.start()


def main(command, path, action, focus, sleeptime, ignorelist=None):
        if action == 'restart':
            ev = RestartHandler(command, path=path, focus=focus,
                                sleeptime=sleeptime,
                                ignorelist=ignorelist)
        else:
            raise NotImplementedError('action %s not implemented' % action)

        ob = Observer()
        ob.schedule(ev, path=path, recursive=True)
        ob.start()
        try:
            while True:
                sleep(1)
        except KeyboardInterrupt:
            ob.stop()
        ob.join()


if __name__ == '__main__':
    arguments = docopt(__doc__, version='Reloader 0.1')
    kwargs = {}
    for k, v in arguments.iteritems():

        k = k.replace('--', '')
        kwargs[k] = v

    print(arguments)

    #main(args.command, args.path, args.action, args.focus, args.sleep,
    #     args.ignorelist)
