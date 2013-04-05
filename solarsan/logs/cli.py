
from solarsan.cli.backend import AutomagicNode
import sh
import errno
import time


class LogsNode(AutomagicNode):
    def __init__(self):
        super(LogsNode, self).__init__()

    def ui_command_tail(self, grep=None):
        '''
        tailf - Tails (and follows) syslog
        '''
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
