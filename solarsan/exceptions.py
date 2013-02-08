
from solarsan.utils.exceptions import FormattedException


class ConnectionError(FormattedException):
    pass


#class DeadPeer(ConnectionError):
#    pass


class TimeoutError(ConnectionError):
    pass
