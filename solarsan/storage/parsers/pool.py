import sh
import yaml
from collections import defaultdict, OrderedDict
from pyparsing import *


class ZpoolStatusParser(object):
    """Parses 'zpool status' output
    # TODO status -v (verbose)

    p = ZpoolStatusParser()
    ret = p()
    pp(ret)
    """

    p_section_content = Combine(restOfLine + ZeroOrMore(LineEnd()
                                + Suppress(White(exact=8)) + restOfLine))
    p_section = Suppress(':') + p_section_content

    p_first_section = Word('pool') + p_section
    p_not_first_section = NotAny(Word('pool')) + Word(alphas) + p_section

    p_pool = Dict(Group(p_first_section)) + OneOrMore(Dict(Group(p_not_first_section)))
    p_status = OneOrMore(Dict(Group(p_pool)))

    def __call__(self, arg=None):
        if not arg:
            arg = sh.zpool('status', '-v').stdout
        ret = OrderedDict()
        for v in self.p_status.parseString(str(arg)).asList():
            v = dict(v)
            v['config'] = self._config_parser(v)
            pool_name = v.pop('pool')
            ret[pool_name] = v
        return ret

    p_config_line = Dict(Group(Word(srange("[-:0-9A-z.]")).setResultsName('name')
                         + Word(alphanums).setResultsName('state')
                         + Word(alphanums).setResultsName('read')
                         + Word(alphanums).setResultsName('write')
                         + Word(alphanums).setResultsName('cksum')
                         + Suppress(LineEnd())))
    p_config = Suppress(restOfLine + LineEnd()) + OneOrMore(p_config_line)

    def _config_parser(self, arg):
        # Config parse for multi-pool (short)
        # TODO handle children / whitespace in front
        ret = OrderedDict()
        for v in self.p_config.parseString(str(arg['config'])):
            ret[v['name']] = v.asDict()
        return ret


class ZdbPoolCacheParser(object):
    def __call__(self):
        """ Snags pool status and vdev info from zdb as zpool status kind of sucks """
        zargs = ['-C', '-v']
        #if pool_name:
        #    zargs.append(pool_name)
        zdb = sh.zdb(*zargs)
        ret = yaml.safe_load(zdb.stdout)
        return ret
