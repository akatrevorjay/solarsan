
from blinker import signal

startup = signal('startup')
shutdown = signal('shutdown')
reboot = signal('reboot')
