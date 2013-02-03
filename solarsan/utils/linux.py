
from .files import fread
import subprocess
import ethtool
import ipaddr


"""
Gracefully lifted from rtslib.utils
"""


def list_loaded_kernel_modules():
    '''
    List all currently loaded kernel modules
    '''
    return [line.split(" ")[0] for line in
            fread("/proc/modules").split('\n') if line]


def modprobe(module):
    '''
    Load the specified kernel module if needed.
    @param module: The name of the kernel module to be loaded.
    @type module: str
    '''
    if module in list_loaded_kernel_modules():
        return

    try:
        import kmod
        kmod.Kmod().modprobe(module)
    except ImportError:
        process = subprocess.Popen(("modprobe", module),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = process.communicate()
        if process.returncode != 0:
            raise Exception(stderrdata)


def list_eth_ips(ifnames=None):
    '''
    List the IPv4 and IPv6 non-loopback, non link-local addresses (in the
    RFC3330 sense, not addresses attached to lo) of a list of ethernet
    interfaces from the SIOCGIFADDR struct. If ifname is omitted, list all IPs
    of all ifaces excepted for lo.
    '''
    if ifnames is None:
        ifnames = [name for name in ethtool.get_devices() if name != 'lo']
    devcfgs = ethtool.get_interfaces_info(ifnames)

    addrs = []
    for d in devcfgs:
        if d.ipv4_address:
            addrs.append(d.ipv4_address)
        # For IPv6 addresses, we might have more of them on the same device,
        # and only grab global (universe) addresses.
        for ip6 in [a for a in d.get_ipv6_addresses() if a.scope == 'universe']:
            addrs.append(ip6.address)

    return sorted(set(addrs))


def is_ipv4_address(addr):
    try:
        ipaddr.IPv4Address(addr)
    except:
        return False
    else:
        return True


def is_ipv6_address(addr):
    try:
        ipaddr.IPv6Address(addr)
    except:
        return False
    else:
        return True
