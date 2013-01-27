#!/usr/bin/env python

from ..template import env
from jinja2 import Template
from .files import burp


def quick_template(template, context=None, is_file=False, out_file=None):
    if is_file:
        t = env.get_template(template)
    else:
        t = Template(template)
    ret = t.render(context)

    if out_file:
        burp(out_file, ret)

    return ret
