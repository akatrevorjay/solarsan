#!/usr/bin/env python

from django.template import Context, Template
from django.template.loader import get_template
from .files import burp


def config_from_template(template, context=None, is_file=False, out_file=None):
    if is_file:
        t = get_template(template)
    else:
        t = Template(template)
    ctx = Context(context)

    ret = t.render(ctx)

    if out_file:
        burp(out_file, ret)

    return ret
