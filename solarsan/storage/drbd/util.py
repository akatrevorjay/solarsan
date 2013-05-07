
from solarsan.exceptions import DrbdFreeMinorUnavailable
from .parsers import drbd_overview_parser


def drbd_find_free_minor():
    ov = drbd_overview_parser()
    minors = [int(x['minor']) for x in ov.values()]
    for minor in range(0, 9):
        if minor not in minors:
            return minor
    raise DrbdFreeMinorUnavailable("Could not find a free minor available")
