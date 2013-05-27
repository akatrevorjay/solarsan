
import datetime


def get_uuid_datetime(uuid):
    return datetime.datetime.fromtimestamp((uuid.time - 0x01b21dd213814000)*100/1e9)
