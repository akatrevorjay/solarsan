
from solarsan.exceptions import UncleanlinessError
import re


def get_regex_for_name(includes_snapshot=False):
    name_chars = r'A-z0-9_\-'
    mid = includes_snapshot and r'@[%s]+' % name_chars or ''
    return r'^[%s]+%s$' % (name_chars, mid)


def clean_name(name, includes_snapshot=False):
    regex = get_regex_for_name(includes_snapshot=includes_snapshot)
    if not re.match(regex, name):
        raise UncleanlinessError("Name has invalid characters: '%s'", name)
    return name
