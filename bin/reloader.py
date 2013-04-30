#!/usr/bin/env python
"""Reloader
Allows to start a program, and to monitor changes in a folder, when changes are
detected in the folder, the command is restarted.

This can be useful to test a software you are developping and having immediate
feedback.
Or to restart a daemon when configuration or data changes.
Or any other use, the sky is the limit :)

Usage: reloader.py [-p <path>] [-i <ignore>] [-r <regex>] [-R <iregex>] [-s <sleep>] [-d] -- <command>...

    -p <path> --path=<path>             Path to monitor for changes. [default: current directory]
    -i <ignore> --ignore=<ignore>       Files to ignore. (unix glob match) [default: .*.swp,.git]
    -r <regex> --regex=<regex>          Files to ignore. (regex)
    -R <iregex> --iregex=<iregex>       Files to ignore. (regex case insensitive)
    -s <sleep> --sleep=<sleep>          Ignore events for this many seconds after the last restart [default: 10]
    -d --debug                          Debug [default: False]
    <command>                           Command to run and restart

"""
import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
from docopt import docopt
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileMovedEvent, FileModifiedEvent
from subprocess import Popen
from time import time, sleep
from fnmatch import fnmatch
import re
import os


class RestartHandler(FileSystemEventHandler):
    _debug = False

    def __init__(self, command, path, ignore, ignore_re, ignore_ire, sleep_time, debug, **kwargs):
        super(RestartHandler, self).__init__(**kwargs)
        self.command = command
        self.ignore = ignore
        self.ignore_re = ignore_re
        self.ignore_ire = ignore_ire
        self.sleep = sleep_time
        self._debug = debug
        self.start()

    def stop(self):
        self._process.terminate()
        log.error('Terminated process')

    def start(self):
        self._last_restart = time()

        self._process = Popen(self.command)
        log.info('Started %s' % self._process)

    def on_any_event(self, event):
        if self._debug:
            log.debug('event=%s', event)

        if not isinstance(event, (FileModifiedEvent, )):
            return

        if self.sleep and time() < self._last_restart + self.sleep:
            return

        for pat in self.ignore:
            if fnmatch(event.src_path, pat) or isinstance(event, FileMovedEvent) and fnmatch(event.dest_path, pat):
                return

        def do_regex(pat, flags=0):
            #r = re.compile('^' + i.replace('*', '.*') + '$')
            r = re.compile(pat, flags=flags)
            if r.match(event.src_path) or isinstance(event, FileMovedEvent) and r.match(event.dest_path):
                return False
            return True

        for pat in self.ignore_re:
            if not do_regex(pat):
                return

        for pat in self.ignore_ire:
            if not do_regex(pat, re.IGNORECASE):
                return

        log.warning('Restarting due to %s', event)

        self.stop()
        self.start()


def main(arguments):
    log.debug('args=%s', arguments)
    command = arguments['<command>']
    path = arguments['--path']
    if path == 'current directory':
        path = os.curdir
    sleep_time = int(arguments['--sleep'])
    ignore = arguments['--ignore']
    ignore_re = arguments['--regex']
    if ignore_re is None:
        ignore_re = []
    ignore_ire = arguments['--iregex']
    if ignore_ire is None:
        ignore_ire = []
    debug = arguments['--debug']

    ev = RestartHandler(command,
                        path=path,
                        ignore=ignore,
                        ignore_re=ignore_re,
                        ignore_ire=ignore_ire,
                        sleep_time=sleep_time,
                        debug=debug,
                        )

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
    main(arguments)
