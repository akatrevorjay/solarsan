
from .. import logger

#from solarsan import conf
#import logging
#import logging.config

#root = logging.getLogger()
#logger = logging.getLogger('solarsan')

#logging.config.dictConfig(conf.LOGGING)

#logger = logging.getLogger('solarsan')

#formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(module)s %(message)s @%(funcName)s:%(lineno)d')
##formatter = logging.Formatter('%(name)s.%(module)s/%(processName)s[%(process)d]: [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d')
#sl_formatter = logging.Formatter('%(name)s.%(module)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d')
##sl_formatter = logging.Formatter('solarsan/%(name)s.%(module)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d')


#logger = logging.getLogger('solarsan')
#logger.setLevel(logging.DEBUG)

#ch = logging.StreamHandler()
#ch = ConsoleHandler()
#ch.setLevel(logging.DEBUG)

#formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(module)s %(message)s @%(funcName)s:%(lineno)d')
#ch.formatter = formatter

#logger.addHandler(ch)


#sl = SysLogHandler(address='/dev/log')
#sl.setLevel(logging.DEBUG)
#sl.formatter = sl_formatter
##logger.addHandler(sl)

from solarsan import mongo
