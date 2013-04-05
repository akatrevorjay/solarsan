
from solarsan.utils.exceptions import FormattedException


"""
Base
"""


class SolarSanError(FormattedException):
    """Generic SolarSan Error"""
    pass


"""
Common
"""


class UncleanlinessError(SolarSanError):
    """Wash the fuck up!"""
    pass


"""
Service
"""


class ConnectionError(SolarSanError):
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


class ZfsError(SolarSanError):
    """Generic ZFS error"""
    pass


class DeviceHandlerNotFound(SolarSanError):
    """Handler is not found for Device error"""
    pass


"""
Drbd Resource
"""


class DrbdResourceError(SolarSanError):
    """Generic Drbd Resource error"""
    pass
