
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.cli.backend import AutomagicNode
#from .models import Syslog
#from .watch import MongoLogWatcher
#from datetime import timedelta
import errno
import time
import sh


class LogsNode(AutomagicNode):
    def __init__(self):
        AutomagicNode.__init__(self)

    TAIL_LOGS = ['/var/log/debug', '/var/log/syslog']

    def tail(self, grep=None):
        tail = sh.tail.bake('-qF', '/var/log/debug', '/var/log/syslog')
        ccze = sh.ccze.bake('-A')
        if grep:
            grep = sh.grep.bake('--', grep)
            ret = ccze(grep(tail(_piped=True), _piped=True), _iter_noblock=True)
        else:
            ret = ccze(tail(_piped=True), _iter_noblock=True)

        for line in ret:
            if line == errno.EWOULDBLOCK:
                time.sleep(1)
                continue
            yield line.rstrip("\n")

    #def monlog_tail(self, grep=None):
    #    watcher = MongoLogWatcher()

    def ui_command_tail(self):
        '''
        tail - Tails (and follows) syslog
        '''
        return self.tail()

    def ui_command_tail_grep(self, grep):
        '''
        tail - Tails (and follows) syslog, only showing lines that match
        '''
        return self.tail(grep=grep)
