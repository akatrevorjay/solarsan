#!/usr/bin/env python

from django.template import Context, Template


def slurp(fn):
    with open(fn, 'r') as fh:
        ret = fh.read()
    return ret


def burp(fn, data):
    with open(fn, 'w') as fh:
        fh.write(data)


def config_from_template(template, context=None, is_file=False, out_file=None):
    if is_file:
        template = slurp(template)

    t = Template(template)
    ctx = Context(context)

    ret = t.render(ctx)

    if out_file:
        burp(out_file, ret)

    return ret
