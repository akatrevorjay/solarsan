

from ping import Ping


def ping_once(host, timeout_ms=1000, packet_size=55, own_id=None):
    p = Ping(host, timeout=timeout_ms, packet_size=packet_size, own_id=own_id)
    ret = p.do()
    return ret
