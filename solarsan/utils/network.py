
import IPy
import re


CIDR_LOOKUP = (
    '0.0.0.0',
    '128.0.0.0',
    '192.0.0.0',
    '224.0.0.0',
    '240.0.0.0',
    '248.0.0.0',
    '252.0.0.0',
    '254.0.0.0',
    '255.0.0.0',
    '255.128.0.0',
    '255.192.0.0',
    '255.224.0.0',
    '255.240.0.0',
    '255.248.0.0',
    '255.252.0.0',
    '255.254.0.0',
    '255.255.0.0',
    '255.255.128.0',
    '255.255.192.0',
    '255.255.224.0',
    '255.255.240.0',
    '255.255.248.0',
    '255.255.252.0',
    '255.255.254.0',
    '255.255.255.0',
    '255.255.255.128',
    '255.255.255.192',
    '255.255.255.224',
    '255.255.255.240',
    '255.255.255.248',
    '255.255.255.252',
    '255.255.255.254',
    '255.255.255.255',
)

NETMASK_LOOKUP = dict((netmask, cidr) for cidr, netmask in enumerate(CIDR_LOOKUP))


def toggle(cidr_or_netmask):
    if isinstance(cidr_or_netmask, int):
        return cidr_to_netmask(cidr_or_netmask)
    else:
        return netmask_to_cidr(cidr_or_netmask)


def cidr_to_netmask(cidr):
    return CIDR_LOOKUP[cidr]


def netmask_to_cidr(netmask):
    return NETMASK_LOOKUP[netmask]


def is_netmask(arg):
    return bool(arg in NETMASK_LOOKUP)


def is_cidr(arg):
    try:
        arg = int(arg)
        return arg > 0 and len(CIDR_LOOKUP) > arg
    except ValueError:
        return False


def is_ip(arg):
    try:
        IPy.IP(arg)
        return True
    except ValueError:
        return False


def is_network(arg, single_ok=True):
    ret = True
    try:
        #ip = IPy.IP(arg.split('/', 1)[0])
        network = IPy.IP(arg, make_net=True)
        if (not single_ok and network.prefixlen() == 32):
            ret = False
    except ValueError:
        ret = False
    return ret


def is_ip_in_network(network, ip):
    network = IPy.IP(network, make_net=True)
    ip = IPy.IP(ip)
    return bool(network.overlaps(ip))


def is_domain(arg):
    return bool(re.match(r'[a-zA-Z\d-]{,63}(\.[a-zA-Z\d-]{,63})*', arg))
