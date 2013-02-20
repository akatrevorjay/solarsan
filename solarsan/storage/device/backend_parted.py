

raise NotImplemented

from solarsan.utils import convert_bytes_to_human
#import os
import parted

RawDevice = parted.Device


def get_device_by_path(device):
    """Returns parted device for given device file"""
    dev = parted.getDevice(device)
    dsk = parted.Disk(dev)
    return dsk


def get_devices():
    """Enumerates parted devies"""
    return parted.getAllDevices()


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
        ret = set([self._backend_device.path])
        return list(ret)

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
        #return self._backend_device.DriveIsRotational
        return True

    @property
    def is_partitioned(self):
        #return self._backend_device.DeviceIsPartitionTable
        return True

    @property
    def is_mounted(self):
        return self._backend_device.DeviceIsMounted

    @property
    def mount_paths(self):
        return self._backend_device.DeviceMountPaths

    @property
    def is_removable(self):
        #return self._backend_device.DeviceIsRemovable
        return False

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
