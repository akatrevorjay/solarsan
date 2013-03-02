
import re
import sh


"""
The resource-specific output from /proc/drbd contains various pieces of information about the resource:

cs (connection state). Status of the network connection. See Section 6.1.3, "Connection states" for details about the various connection states.

ro (roles). Roles of the nodes. The role of the local node is displayed first, followed by the role of the partner node shown after the slash. See Section 6.1.4, "Resource roles" for details about the possible resource roles.

ds (disk states). State of the hard disks. Prior to the slash the state of the local node is displayed, after the slash the state of the hard disk of the partner node is shown. See Section 6.1.5, "Disk states" for details about the various disk states.

Replication protocol. Replication protocol used by the resource. Either A, B or C. See Section 2.3, "Replication modes" for details.

I/O Flags. Six state flags reflecting the I/O status of this resource. See Section 6.1.6, "I/O state flags" for a detailed explanation of these flags.

Performance indicators. A number of counters and gauges reflecting the resource's utilization and performance. See Section 6.1.7, "Performance indicators" for details.
"""


#def proc_drbd_parser(resource=None):
#    """Drbd /proc/drbd parser"""
#    raise NotImplemented


def drbd_overview_parser(resource=None):
    """Parse drbd-overview which really parses /proc/drbd"""
    cmd = sh.Command('drbd-overview')
    cmd_ret = cmd(_iter=True)

    # There's space at the beginning AND end
    pat = ['',
           '(?P<minor>\d+):(?P<name>[\w\d]+)/\d',
           '(?P<connection_state>\w+)',
           '(?P<role>\w+)/(?P<remote_role>\w+)',
           '(?P<disk_state>\w+)/(?P<remote_disk_state>\w+)',
           '(?P<protocol>\w)',
           '(?P<io_flags>[-\w]+)',
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
