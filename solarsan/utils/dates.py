
import datetime
import time


def datetime2int(dt):
    """Returns microseconds since epoch of the given datetime as int.
    """
    ts = int(float(dt.strftime("%s.%f")) * 1000000)
    return ts


def int2datetime(i):
    """Returns the datetime for the given microseconds since epoch.
    """
    ts = float(i) / 1000000.0
    dt = datetime.datetime.fromtimestamp(ts)
    return dt


def timedelta2int(td):
    """Returns microseconds for given timedelta.
    """
    t = (td.days * 86400 + td.seconds) * 1000000
    return t + td.microseconds


def int2timedelta(i):
    """Returns timedelta for given microseconds.
    """
    secs = i // 1000000
    musec = i - (secs * 1000000)
    return datetime.timedelta(seconds=secs, microseconds=musec)


def timestamp_micro(now=None):
    """Returns the microseconds for the given timestamp.

    If now is not given, time.time() is used.
    """
    if not now:
        now = time.time()
    return int(now * 1000000.0)
