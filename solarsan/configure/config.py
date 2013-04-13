

#from solarsan.core import logger
#from solarsan import conf
from solarsan.template import quick_template
from .models import Nic  # , get_network_config
from solarsan.ha.models import FloatingIP
import os
import shutil


def write_network_interfaces_config(confirm=False):
    """Write out network configuration"""
    context = dict(
        ifaces=[nic.config for nic in Nic.list().values()],
        #ifaces=NicConfig.objects.filter(is_enabled=True),
        #netconf=get_network_config(),
        floating_ips=FloatingIP.objects.all(),
    )
    if confirm:
        fn = '/etc/network/interfaces'
        if os.path.isfile(fn):
            shutil.copyfile(fn, '%s.bak' % fn)
    else:
        fn = None
    return quick_template('interfaces', context=context, write_file=fn)
