
from solarsan.utils.exceptions import FormattedException


"""
Base
"""


class SolarSanError(FormattedException):
    """Generic SolarSan Error"""


"""
Common
"""


class UncleanlinessError(SolarSanError):
    """Wash the fuck up!"""


"""
Service
"""


class ConnectionError(SolarSanError):
    """Generic connection error"""


#class DeadPeer(ConnectionError):
#    pass


class TimeoutError(ConnectionError):
    """Timeout error"""


"""
Storage
"""


class ZfsError(SolarSanError):
    """Generic ZFS error"""


class DeviceHandlerNotFound(SolarSanError):
    """Handler is not found for Device error"""


"""
Drbd Resource
"""


class DrbdError(SolarSanError):
    """Generic Drbd error"""


class DrbdResourceError(DrbdError):
    """Generic Drbd Resource error"""


class DrbdFreeMinorUnavailable(DrbdError):
    """Could not find a free Drbd minor"""
