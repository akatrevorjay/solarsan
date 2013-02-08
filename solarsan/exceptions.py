
from solarsan.utils.exceptions import FormattedException


"""
Service
"""


class ConnectionError(FormattedException):
    pass


#class DeadPeer(ConnectionError):
#    pass


class TimeoutError(ConnectionError):
    pass


"""
Storage
"""


class ZfsError(FormattedException):
    pass
