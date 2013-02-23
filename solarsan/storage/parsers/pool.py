
#from solarsan.core import logger
import sh
import yaml
import re
from collections import defaultdict


def zpool_status_parse(from_string=None):
    if not from_string:
        from_string = sh.zpool('status', '-v').stdout

    ret = defaultdict(dict)

    pools_m = from_string.split("pool:")
    for pool_m in pools_m:
        if not pool_m.strip():
            continue

        for m in re.finditer(" (?P<pool>[^\n]+)\n *"  # We've split on pool:, so our first word is the pool name
                             "state: (?P<state>[^ ]+)\n *"
                             "(status: (?P<status>(.|\n)+)\n *)??"
                             "scan: (?P<scan>(.|\n)*)\n *"
                             "config: ?(?P<config>(.|\n)*)\n *"
                             "errors: (?P<errors>[^\n]*)",
                             pool_m):
            m = m.groupdict()
            pool_name = m.pop('pool')
            pool = ret[pool_name] = m

            disks = pool['devices'] = {}
            parent = None
            for disk in re.finditer("(?P<indent>[ \t]+)(?P<name>[^ \t\n]+)( +(?P<state>[^ \t\n]+) +)?("
                                    "(?P<read>[^ \t\n]+) +(?P<write>[^ \t\n]+) +"
                                    "(?P<cksum>[^\n]+))?(?P<notes>[^\n]+)?\n",
                                    pool.pop('config')):
                disk = disk.groupdict()
                if not disk['name'] or disk['name'] in ("NAME", pool_name):
                    continue
                disk_name = disk.pop('name').strip()
                disk.pop('indent')

                is_parent = False
                for disk_type in ('mirror', 'log', 'raid', 'spare', 'cache'):
                    if disk_name.startswith(disk_type):
                        parent = disk_name
                        disks[disk_name] = disk
                        disks[parent]['children'] = {}
                        is_parent = True
                        break
                if is_parent:
                    continue

                if parent:
                    disks[parent]['children'][disk_name] = disk
                else:
                    disks[disk_name] = disk

    return dict(ret)


class ZdbPoolCacheParser(object):
    def __call__(self):
        """ Snags pool status and vdev info from zdb as zpool status kind of sucks """
        zargs = ['-C', '-v']
        #if pool_name:
        #    zargs.append(pool_name)
        zdb = sh.zdb(*zargs)
        ret = yaml.safe_load(zdb.stdout)
        return ret
