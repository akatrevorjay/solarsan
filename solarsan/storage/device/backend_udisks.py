from solarsan.utils import convert_bytes_to_human
import os
import udisks
_udisks = udisks.UDisks()

RawDevice = udisks.device.Device


def get_device_by_path(path):
    """Returns udisks device for given device path"""
    dbus_obj = _udisks.iface.FindDeviceByDeviceFile(path)
    if dbus_obj:
        return udisks.device.Device(dbus_obj)


GUESS_DEV_PATHS = ['/dev/disk/by-uuid', '/dev/disk/by-id', '/dev/disk/by-path', '/dev/disk/by-label',
                   '/dev/disk/by-partlabel', '/dev/disk/by-partuuid', '/dev']


def guess_device_path(name_or_path):
    """Guesses device path for a basename of a device path"""
    name_or_path = os.path.basename(name_or_path)
    for path in GUESS_DEV_PATHS:
        try_path = os.path.join(path, name_or_path)
        if os.path.exists(try_path):
            return try_path


def get_devices():
    """Enumerates udisks devices"""
    return _udisks.EnumerateDevices()


class BaseDevice(object):
    """
    Device Info
    """

    # Vendor is apparently 'ATA' ? Doesn't make sense, not including this for
    # now. If needed just split(self.model)[0].
    #@property
    #def vendor(self):
    #    return self._backend_device.DriveVendor

    @property
    def model(self):
        return self._backend_device.DriveModel

    @property
    def revision(self):
        return self._backend_device.DriveRevision

    @property
    def serial(self):
        return self._backend_device.DriveSerial

    # Partitions only
    #@property
    #def uuid(self):
    #    return self._backend_device.DriveUuid

    @property
    def wwn(self):
        return self._backend_device.DriveWwn

    def size(self, human=False):
        ret = self._backend_device.DeviceSize
        if human:
            ret = convert_bytes_to_human(ret)
        return ret

    @property
    def block_size(self):
        return self._backend_device.DeviceBlockSize

    def paths(self, by_id=True, by_path=True):
        ret = set([self._backend_device.DeviceFile])
        if by_id:
            ret.update(self._backend_device.DeviceFileById)
        if by_path:
            ret.update(self._backend_device.DeviceFileByPath)
        return list(ret)

    """
    Id fields
    """

    @property
    def id_label(self):
        """Label; For ZFS this is the Pool name"""
        return self._backend_device.IdLabel

    zpool_name = id_label

    @property
    def id_type(self):
        """Type; For ZFS members this is, surprisingly, zfs_member"""
        return self._backend_device.IdType

    @property
    def is_zfs_member(self):
        return self.id_type == 'zfs_member'

    @property
    def id_usage(self):
        """Usage; For ZFS members this is raid"""
        return self._backend_device.IdUsage

    @property
    def id_uuid(self):
        """UUID: For ZFS members this is the root vdev guid of the Pool"""
        return self._backend_device.IdUuid

    #zpool_root_vdev_guid = id_uuid
    zpool_guid = id_uuid

    @property
    def id_version(self):
        """Version: For ZFS members this is the pool version"""
        return self._backend_device.IdVersion

    zpool_version = id_version

    """
    SMART
    """

    @property
    def smart_status(self):
        return self._backend_device.DriveAtaSmartStatus

    @property
    def is_smart_available(self):
        return self._backend_device.DriveAtaSmartIsAvailable

    # Not yet implemented in udisks OR in python-udisks
    #def smart_self_test(self):
    #    return self._backend_device.DriveAtaInitiateSelfTest()

    """
    Device Properties
    """

    @property
    def is_rotational(self):
        return self._backend_device.DriveIsRotational

    @property
    def is_partitioned(self):
        return self._backend_device.DeviceIsPartitionTable

    @property
    def is_mounted(self):
        return self._backend_device.DeviceIsMounted

    @property
    def mount_paths(self):
        return self._backend_device.DeviceMountPaths

    @property
    def is_removable(self):
        return self._backend_device.DeviceIsRemovable

    @property
    def is_readonly(self):
        return self._backend_device.DeviceIsReadOnly

    """
    Hmm, not sure if these even belong here
    """

    @property
    def is_drive(self):
        return self._backend_device.DeviceIsDrive

    @property
    def is_partition(self):
        return self._backend_device.DeviceIsPartition

    """
    LVM2
    """

    @property
    def is_lvm2_lv(self):
        return self._backend_device.DeviceIsLinuxLvm2LV

    @property
    def is_lvm2_pv(self):
        return self._backend_device.DeviceIsLinuxLvm2PV

    """
    mdraid
    """

    @property
    def is_mdraid(self):
        return self._backend_device.DeviceIsLinuxMd

    @property
    def is_mdraid_degraded(self):
        return self._backend_device.LinuxMdIsDegraded

    @property
    def is_mdraid_component(self):
        return self._backend_device.DeviceIsLinuxMdComponent
