
from jinja2 import Environment, PackageLoader, Template
env = Environment(loader=PackageLoader('solarsan', 'templates'))

from .utils.files import burp


def quick_template(template, context=None, is_string=False, write_file=None):
    if is_string:
        t = Template(template)
    else:
        t = env.get_template(template)

    ret = t.render(context)

    if write_file:
        burp(write_file, ret)
        return True
    else:
        return ret
