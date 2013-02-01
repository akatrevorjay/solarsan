
#from solarsan.models import Config
import socket

hostname = socket.gethostname()

LOGGING = {
    'version': 1,
    #'disable_existing_loggers': True,
    #'disable_existing_loggers': False,
    'root': {
        'level': 'DEBUG',
        'handlers': ['console', 'syslog'],
        #'level': 'WARNING',
        #'handlers': ['console', 'sentry'],
    },

    'formatters': {
        'standard': {
            'format': '%(asctime)s %(levelname)s %(name)s.%(module)s@%(funcName)s:%(lineno)d %(message)s',
            #'datefmt': '%d/%b/%Y %H:%M:%S',
        },
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'syslog': {
            #'format': '<22>%(asctime)s ' + SERVER_NAME + ' %(name)s[%(process)d]: %(message)s',
            #'format': 'solarsan.%(name)s[%(process)d]: %(levelname)s %(message)s',
            #'format': 'solarsan/%(name)s.%(module)s/%(processName)s[%(process)d]: %(levelname)s %(message)s @%(funcName)s:%(lineno)d',
            'format': 'solarsan/%(name)s.%(module)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d',
            #'celery_format': 'solarsan/%(name)s[%(process)d]: %(levelname)s %(message)s @%(funcName)s:%(lineno)d',
        },
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse',
        },
    },
    'handlers': {
        #'sentry': {
        #    'level': 'DEBUG',
        #    'class': 'raven.contrib.django.handlers.SentryHandler',
        #},
        #'mail_admins': {
        #    'level': 'ERROR',
        #    'class': 'django.utils.log.AdminEmailHandler',
        #},
        'null': {
            'level': 'DEBUG',
            'class': 'django.utils.log.NullHandler',
        },
        'console': {
            'level': 'DEBUG',
            #'class': 'logging.StreamHandler',
            'class': 'ConsoleHandler.ConsoleHandler',
            'formatter': 'standard',
        },
        'syslog': {
            'level': 'DEBUG',
            'class': 'logging.handlers.SysLogHandler',
            'formatter': 'syslog',
            'address': '/dev/log',
        },
    },
    'loggers': {
        'django.db': {
            'propagate': True,
            #'level': 'DEBUG',
            'level': 'INFO',
        },
    }
}

rpyc_conn_config = {
    'allow_exposed_attrs': False,
    'allow_public_attrs': True,
    'allow_all_attrs': True,
    'allow_setattr': True,
    'allow_delattr': True,
    #'exposed_prefix': '',
    'include_local_traceback': True,
}

auto_snap = {
}

scst_config_file = '/etc/scst.conf'

#def get(name):
#    created, ret = Config.objects.get(name=name)
#    return ret
