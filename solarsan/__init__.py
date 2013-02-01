import logging.config
from .conf import LOGGING
logging.config.dictConfig(LOGGING)
root = logging.getLogger()
logger = logging.getLogger(__name__)
