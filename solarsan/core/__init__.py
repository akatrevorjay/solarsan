
import logging

logger = logging.getLogger('solarsan')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(module)s %(message)s @%(funcName)s:%(lineno)d')
#formatter = logging.Formatter('%(name)s.%(module)s/%(processName)s[%(process)d]: [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d')
ch.formatter = formatter
logger.addHandler(ch)

from solarsan import mongo
