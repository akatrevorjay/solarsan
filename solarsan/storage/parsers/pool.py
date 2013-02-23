
#from solarsan.core import logger
import sh
import yaml
import re
from storage.device import Device, Cache, Log, Spare, Mirror


def zpool_status_parse(from_string=None):
    if not from_string:
        from_string = sh.zpool('status', '-v').stdout

    ret = {}

    pools_m = from_string.split("pool:")
    for pool_m in pools_m:
        if not pool_m.strip():
            continue

        for m in re.finditer(" (?P<pool>[^\n]+)\n *"
                             "state: (?P<state>[^ ]+)\n *"
                             "(status: (?P<status>(.|\n)+)\n *)??"
                             "scan: (?P<scan>(.|\n)*)\n *"
                             "config: ?(?P<config>(.|\n)*)\n *"
                             "errors: (?P<errors>[^\n]*)",
                             pool_m):
            m = m.groupdict()
            pool_name = m.pop('pool')
            pool = ret[pool_name] = m

            devices = pool['devices'] = {}
            devices2 = pool['devices2'] = {}
            parent = None
            parent_type = None
            for device in re.finditer("(?P<indent>[ \t]+)(?P<name>[^ \t\n]+)( +(?P<state>[^ \t\n]+) +)?("
                                      "(?P<read>[^ \t\n]+) +(?P<write>[^ \t\n]+) +"
                                      "(?P<cksum>[^\n]+))?(?P<notes>[^\n]+)?\n",
                                      pool.pop('config')):
                device = device.groupdict()
                if not device['name'] or device['name'] in ("NAME", pool_name):
                    continue
                device_name = device.pop('name').strip()
                device.pop('indent')

                is_parent = False
                for device_type in ('mirror', 'log', 'raid', 'spare', 'cache'):
                    if device_name.startswith(device_type):
                        parent = device_name
                        parent_type = device_type
                        devices[device_name] = device
                        devices[parent]['children'] = {}

                        if device_type == 'mirror':
                            devices2[device_name] = Mirror()

                        is_parent = True
                        break
                if is_parent:
                    continue

                # TODO May fail if device is not found
                device_type_map = {'log': Log, 'cache': Cache, 'spare': Spare}
                if parent_type in device_type_map:
                    dev = device_type_map[parent_type](device_name)
                else:
                    dev = Device(device_name)

                if parent:
                    devices[parent]['children'][device_name] = device
                    if parent_type == 'mirror':
                        devices2[parent].append(dev)
                else:
                    devices[device_name] = device
                    devices2[device_name] = dev

    return ret


def zdb_pool_cache_parse():
    """ Snags pool status and vdev info from zdb as zpool status kind of sucks """
    zargs = ['-C', '-v']
    #if pool_name:
    #    zargs.append(pool_name)
    zdb = sh.zdb(*zargs)
    ret = yaml.safe_load(zdb.stdout)
    return ret
