
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
    """Generic connection error"""
    pass


#class DeadPeer(ConnectionError):
#    pass


class TimeoutError(ConnectionError):
    """Timeout error"""
    pass


"""
Storage
"""


class ZfsError(FormattedException):
    """Generic ZFS error"""
    pass


class DeviceHandlerNotFound(FormattedException):
    """Handler is not found for Device error"""
    pass
