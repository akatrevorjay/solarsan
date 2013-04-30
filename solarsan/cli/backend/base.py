

import inspect
from decorator import getfullargspec


class AutomagicNode(object):
    def summary(self):
        return (None, None)

    def get_ui_commands(self):
        for attr in dir(self):
            if not attr.startswith('ui_command_'):
                continue
            val = getattr(self, attr)
            argspec = getfullargspec(val)
            yield (
                attr,
                dict(
                    argspec=argspec,
                    method=inspect.ismethod(val),
                    function=inspect.isfunction(val),
                )
            )

    def get_ui_children(self):
        for attr in dir(self):
            if attr.startswith('ui_children_factory_'):
                if attr.endswith('_list'):
                    factory = attr.rpartition('_list')[0]

                    func = getattr(self, attr)
                    for name in func():
                        service_config = dict(factory=factory)
                        yield (name, service_config)
                elif attr.endswith('_dict'):
                    factory = attr.rpartition('_dict')[0]

                    func = getattr(self, attr)
                    for display_name, service_config in func().iteritems():
                        service_config['factory'] = factory
                        yield (display_name, service_config)

            elif attr.startswith('ui_child_'):
                name = attr.partition('ui_child_')[2]
                yield (name, {})

    _config_group_params = {}

    def define_config_group_param(self, group_name, name, type, desc=None, writable=True):
        if group_name not in self._config_group_params:
            self._config_group_params[group_name] = {}
        self._config_group_params[group_name][name] = dict(desc=desc, writable=writable, type=type)

    def get_ui_config_groups(self):
        ret = {}
        for attr in dir(self):
            if not (attr.startswith('ui_setgroup_') or attr.startswith('ui_getgroup_')):
                continue
            getset, name = attr.split('_', 2)[1:]
            getset = getset[:3]
            if name not in ret:
                ret[name] = {}
            ret[name][getset] = True

            params = self._config_group_params.get(name)
            if params:
                ret[name]['params'] = params
        return ret
