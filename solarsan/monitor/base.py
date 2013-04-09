
from setproctitle import setproctitle


def set_proc_status(append=None):
    title = 'SolarSan Monitor'
    if append:
        title += ': %s' % append
    setproctitle('[%s]' % title)
