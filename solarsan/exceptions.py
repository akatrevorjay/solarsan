
from solarsan.utils.exceptions import FormattedException


"""
Common
"""


class UncleanlinessError(FormattedException):
    """Wash the fuck up!"""
    pass


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
