
import re
import sh


def drbd_overview_parser(resource=None):
    cmd = sh.Command('drbd-overview')
    cmd_ret = cmd(_iter=True)

    # There's space at the beginning AND end
    pat = ['',
           '(?P<minor>\d+):(?P<name>[\w\d]+)/\d',
           '(?P<connection_state>\w+)',
           '(?P<role>\w+)/(?P<remote_role>\w+)',
           '(?P<disk_state>\w+)/(?P<remote_disk_state>\w+)',
           '\w',
           '[-\w]+',
           '',
           ]
    pat = r'^%s$' % '\s+'.join(pat)

    ret = {}
    for line in cmd_ret:
        line = line.rstrip("\n")
        m = re.match(pat, line)

        #if not m:
        #    m = re.match(pat2, line)

        if m:
            m = m.groupdict()

            if resource and resource == m['name']:
                return m

            ret[m['name']] = m
    if not resource:
        return ret
