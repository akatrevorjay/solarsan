
#from solarsan.core import logger
import sh
import yaml
import re
from ..device import Device, Cache, Log, Spare, Mirror
#from solarsan.pretty import pp


# TODO Merge this into the original
def zpool_status_parse2(from_string=None):
    if not from_string:
        from_string = sh.zpool('status', '-v').stdout

    ret = {}

    pools_m = from_string.split("pool:")
    for pool_m in pools_m:
        if not pool_m.strip():
            continue

        for m in re.finditer(" (?P<pool>[^\n]+)\n *"
                             "state: (?P<state>[^ ]+)\n *"
                             # Some versions of zpool use tabs, some do not.
                             "(status: (?P<status>(.|\n {8}|\n\t)+)\n *)??"
                             "(action: (?P<action>(.|\n {8}|\n\t)+)\n *)??"
                             "(see: (?P<see>(.|\n {8}|\n\t)+)\n *)??"
                             "scan: (?P<scan>(.|\n)*)\n *"
                             "config: ?(?P<config>(.|\n)*)\n *"
                             "errors: (?P<errors>[^\n]*)",
                             pool_m):
            m = m.groupdict()

            # Remove nasty newlines and prefixing whitespace
            for k, v in m.items():
                if k == 'config' or not v:
                    continue
                # Some versions of zpool use tabs, some do not.
                m[k] = v.replace("\n\t", " ")
                m[k] = m[k].replace("\n        ", " ")

            pool_name = m.pop('pool')
            pool = ret[pool_name] = m

            devices = pool['devices'] = {}

            _devices = [d.groupdict() for d in re.finditer("(?P<indent>[ \t]+)(?P<name>[^ \t\n]+)( +(?P<state>[^ \t\n]+) +)?("
                                                           "(?P<read>[^ \t\n]+) +(?P<write>[^ \t\n]+) +"
                                                           "(?P<cksum>[^\n]+))?(?P<notes>[^\n]+)?\n",
                                                           pool.pop('config'))]
            _devices = filter(lambda d: d['name'] and not d['name'] == 'NAME', _devices)

            ancestry = []
            #ancestry_level = 0
            dev_types = {'devices': Device,
                         'logs': Log,
                         'cache': Cache,
                         'spare': Spare, }
            dev_type_cls = None

            for device in _devices:
                device_name = device.pop('name').strip()
                # Some versions of zpool use tabs, some do not.
                cur_level = (len(device['indent'].replace("\t", '        ')) - 8) / 2
                #level_diff = cur_level - ancestry_level
                ancestry = ancestry[:cur_level]
                ancestry.append(device_name)
                #ancestry_level = cur_level
                #pp(dict(name=device_name, cur_level=cur_level, level_diff=level_diff, ancestry=ancestry))

                if cur_level == 0 and device_name in dev_types.keys() or device_name == pool_name:
                    if device_name == pool_name:
                        device_name = 'devices'
                    dev_type_cls = dev_types.pop(device_name)
                    ancestry.append(device_name)
                    continue

                cur = None
                for level in ancestry[:cur_level]:
                    #print level
                    if not cur:
                        cur = devices
                        continue
                    if not level in cur:
                        cur[level] = {}
                    cur = cur[level]

                if device_name.startswith('mirror-'):
                    cur[device_name] = Mirror()
                    continue

                dev = dev_type_cls(device_name)

                if isinstance(cur, Mirror):
                    cur.append(dev, _device_check=False)
                else:
                    cur[device_name] = dev

    return ret


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

            parent = None
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
                        devices[device_name] = device
                        devices[parent]['children'] = {}
                        is_parent = True
                        break
                if is_parent:
                    continue

                if parent:
                    devices[parent]['children'][device_name] = device
                else:
                    devices[device_name] = device

    return ret


def zdb_pool_cache_parse():
    """ Snags pool status and vdev info from zdb as zpool status kind of sucks """
    zargs = ['-C', '-v']
    #if pool_name:
    #    zargs.append(pool_name)
    zdb = sh.zdb(*zargs)
    ret = yaml.safe_load(zdb.stdout)
    return ret
