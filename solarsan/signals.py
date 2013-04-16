
from blinker import signal


"""
MongoEngine signals
"""

#from mongoengine.signals import pre_bulk_insert, pre_delete, pre_init, pre_save, \
#    post_bulk_insert, post_delete, post_init, post_save
pre_bulk_insert = signal('pre_bulk_insert')
pre_delete = signal('pre_delete')
pre_init = signal('pre_init')
pre_save = signal('pre_save')
post_bulk_insert = signal('post_bulk_insert')
post_delete = signal('post_delete')
post_init = signal('post_init')
post_save = signal('post_save')


"""
SolarSan signals
"""

startup = signal('startup')
shutdown = signal('shutdown')
reboot = signal('reboot')

pre_start = signal('pre_start')
pre_stop = signal('pre_stop')
post_start = signal('post_start')
post_stop = signal('post_stop')

check_log_entry = signal('check_log_entry')
