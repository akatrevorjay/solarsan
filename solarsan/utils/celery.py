"""
Celery scheduler that runs tasks at startup immediately then continues with their
original plan.
"""

'''
from djcelery import schedulers


class ImmediateFirstEntry( schedulers.ModelEntry ):
    def is_due( self ):
        if self.last_run_at is None:
            return True, 0
        return super( ImmediateFirstEntry, self ).is_due()
    def _default_now( self ):
        return None


class CeleryBeatScheduler( schedulers.DatabaseScheduler ):
    Entry = ImmediateFirstEntry
'''
