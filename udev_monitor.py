#!/usr/bin/env python

import time
import logging
logging.basicConfig(level='DEBUG')
import re

import pyudev

context = pyudev.Context()

'''
Example props on disk:

{u'DEVLINKS': u'/dev/disk/by-path/pci-0000:00:06.0-virtio-pci-virtio2',
 u'DEVNAME': u'/dev/vda',
 u'DEVPATH': u'/devices/pci0000:00/0000:00:06.0/virtio2/block/vda',
 u'DEVTYPE': u'disk',
 u'ID_PART_TABLE_TYPE': u'dos',
 u'ID_PATH': u'pci-0000:00:06.0-virtio-pci-virtio2',
 u'ID_PATH_TAG': u'pci-0000_00_06_0-virtio-pci-virtio2',
 u'MAJOR': u'253',
 u'MINOR': u'0',
 u'SUBSYSTEM': u'block',
 u'UDEV_LOG': u'3',
 u'UDISKS_PARTITION_TABLE': u'1',
 u'UDISKS_PARTITION_TABLE_COUNT': u'3',
 u'UDISKS_PARTITION_TABLE_SCHEME': u'mbr',
 u'UDISKS_PRESENTATION_NOPOLICY': u'1',
 u'USEC_INITIALIZED': u'592920'}

Partition on same disk:

{u'DEVLINKS': u'/dev/disk/by-label/bootfs /dev/disk/by-path/pci-0000:00:06.0-virtio-pci-virtio2-part1 /dev/disk/by-uuid/244f829f-a003-4b00-b9ef
-2bb67defbef1',
 u'DEVNAME': u'/dev/vda1',
 u'DEVPATH': u'/devices/pci0000:00/0000:00:06.0/virtio2/block/vda/vda1',
 u'DEVTYPE': u'partition',
 u'ID_FS_LABEL': u'bootfs',
 u'ID_FS_LABEL_ENC': u'bootfs',
 u'ID_FS_TYPE': u'ext3',
 u'ID_FS_USAGE': u'filesystem',
 u'ID_FS_UUID': u'244f829f-a003-4b00-b9ef-2bb67defbef1',
 u'ID_FS_UUID_ENC': u'244f829f-a003-4b00-b9ef-2bb67defbef1',
 u'ID_FS_VERSION': u'1.0',
 u'ID_PART_ENTRY_DISK': u'253:0',
 u'ID_PART_ENTRY_FLAGS': u'0x80',
 u'ID_PART_ENTRY_NUMBER': u'1',
 u'ID_PART_ENTRY_OFFSET': u'2048',
 u'ID_PART_ENTRY_SCHEME': u'dos',
 u'ID_PART_ENTRY_SIZE': u'497664',
 u'ID_PART_ENTRY_TYPE': u'0x83',
 u'ID_PART_TABLE_TYPE': u'dos',
 u'ID_PATH': u'pci-0000:00:06.0-virtio-pci-virtio2',
 u'ID_PATH_TAG': u'pci-0000_00_06_0-virtio-pci-virtio2',
 u'MAJOR': u'253',
 u'MINOR': u'1',
 u'SUBSYSTEM': u'block',
 u'UDEV_LOG': u'3',
 u'UDISKS_PARTITION': u'1',
 u'UDISKS_PARTITION_ALIGNMENT_OFFSET': u'0',
 u'UDISKS_PARTITION_FLAGS': u'boot',
 u'UDISKS_PARTITION_NUMBER': u'1',
 u'UDISKS_PARTITION_OFFSET': u'1048576',
 u'UDISKS_PARTITION_SCHEME': u'mbr',
 u'UDISKS_PARTITION_SIZE': u'254803968',
 u'UDISKS_PARTITION_SLAVE': u'/sys/devices/pci0000:00/0000:00:06.0/virtio2/block/vda',
 u'UDISKS_PARTITION_TYPE': u'0x83',
 u'UDISKS_PRESENTATION_NOPOLICY': u'1',
 u'USEC_INITIALIZED': u'662452'}

Example props on disk in zpool:

{u'DEVLINKS': u'/dev/disk/by-path/pci-0000:00:07.0-virtio-pci-virtio3',
 u'DEVNAME': u'/dev/vdb',
 u'DEVPATH': u'/devices/pci0000:00/0000:00:07.0/virtio3/block/vdb',
 u'DEVTYPE': u'disk',
 u'ID_PART_TABLE_TYPE': u'gpt',
 u'ID_PATH': u'pci-0000:00:07.0-virtio-pci-virtio3',
 u'ID_PATH_TAG': u'pci-0000_00_07_0-virtio-pci-virtio3',
 u'MAJOR': u'253',
 u'MINOR': u'16',
 u'SUBSYSTEM': u'block',
 u'UDEV_LOG': u'3',
 u'UDISKS_PARTITION_TABLE': u'1',
 u'UDISKS_PARTITION_TABLE_COUNT': u'2',
 u'UDISKS_PARTITION_TABLE_SCHEME': u'gpt',
 u'UDISKS_PRESENTATION_NOPOLICY': u'1',
 u'USEC_INITIALIZED': u'593065'}

Example props on partition of disk in zpool above:

{u'DEVLINKS': u'/dev/disk/by-partlabel/zfs /dev/disk/by-partuuid/88f38235-8a41-b14e-adcf-2d1a31decd39 /dev/disk/by-path/pci-0000:00:07.0-virtio-pci-virtio3-part1',
 u'DEVNAME': u'/dev/vdb1',
 u'DEVPATH': u'/devices/pci0000:00/0000:00:07.0/virtio3/block/vdb/vdb1',
 u'DEVTYPE': u'partition',
 u'ID_FS_LABEL': u'dpool',
 u'ID_FS_LABEL_ENC': u'dpool',
 u'ID_FS_TYPE': u'zfs_member',
 u'ID_FS_USAGE': u'raid',
 u'ID_FS_UUID': u'4322943791351954282',
 u'ID_FS_UUID_ENC': u'4322943791351954282',
 u'ID_FS_UUID_SUB': u'16763412353620561694',
 u'ID_FS_UUID_SUB_ENC': u'16763412353620561694',
 u'ID_FS_VERSION': u'28',
 u'ID_PART_ENTRY_DISK': u'253:16',
 u'ID_PART_ENTRY_NAME': u'zfs',
 u'ID_PART_ENTRY_NUMBER': u'1',
 u'ID_PART_ENTRY_OFFSET': u'2048',
 u'ID_PART_ENTRY_SCHEME': u'gpt',
 u'ID_PART_ENTRY_SIZE': u'16756736',
 u'ID_PART_ENTRY_TYPE': u'6a898cc3-1dd2-11b2-99a6-080020736631',
 u'ID_PART_ENTRY_UUID': u'88f38235-8a41-b14e-adcf-2d1a31decd39',
 u'ID_PART_TABLE_TYPE': u'gpt',
 u'ID_PATH': u'pci-0000:00:07.0-virtio-pci-virtio3',
 u'ID_PATH_TAG': u'pci-0000_00_07_0-virtio-pci-virtio3',
 u'MAJOR': u'253',
 u'MINOR': u'17',
 u'SUBSYSTEM': u'block',
 u'UDEV_LOG': u'3',
 u'UDISKS_PARTITION': u'1',
 u'UDISKS_PARTITION_ALIGNMENT_OFFSET': u'0',
 u'UDISKS_PARTITION_LABEL': u'zfs',
 u'UDISKS_PARTITION_NUMBER': u'1',
 u'UDISKS_PARTITION_OFFSET': u'1048576',
 u'UDISKS_PARTITION_SCHEME': u'gpt',
 u'UDISKS_PARTITION_SIZE': u'8579448832',
 u'UDISKS_PARTITION_SLAVE': u'/sys/devices/pci0000:00/0000:00:07.0/virtio3/block/vdb',
 u'UDISKS_PARTITION_TYPE': u'6A898CC3-1DD2-11B2-99A6-080020736631',
 u'UDISKS_PARTITION_UUID': u'88F38235-8A41-B14E-ADCF-2D1A31DECD39',
 u'UDISKS_PRESENTATION_NOPOLICY': u'1',
 u'USEC_INITIALIZED': u'647804'}

'''


def get_device(path=None, subsystem=None, name=None, device_file=None):
    if path:
        return pyudev.Device.from_path(context, path)
    elif name:
        if not subsystem:
            raise Exception('Incorrect arguments: No subsystem specified')
        return pyudev.Device.from_name(context, subsystem, name)
    elif device_file:
        return pyudev.Device.from_device_file(context, device_file)
    else:
        raise Exception('Incorrect arguments')


def get_block_devices(name__match=None, ram=False, zd=False, loop=False, drbd=False, disks=True):
    ret = []
    for device in context.list_devices(subsystem='block'):
        name = device.get('DEVNAME')
        if not name:
            continue

        if name__match:
            if not re.match(name__match, name):
                continue
        else:
            if name.startswith('/dev/ram'):
                if not ram:
                    continue
            elif name.startswith('/dev/zd'):
                if not zd:
                    continue
            elif name.startswith('/dev/loop'):
                if not loop:
                    continue
            elif name.startswith('/dev/drbd'):
                if not drbd:
                    continue
            else:
                if not disks:
                    continue

        ret.append(device)
    return ret


def list_block_devices():
    for device in get_block_devices():
        print '{0} ({1})'.format(device.device_node, device.device_type)
        #print '{0} ({1})'.format(device['DEVNAME'], device['DEVTYPE'])


def list_block_disks(ram=False, zd=False, loop=False, drbd=True, ):
    for device in context.list_devices(subsystem='block', DEVTYPE='disk'):
        print '{0} ({1})'.format(device.device_node, device.device_type)
        #print '{0} ({1})'.format(device['DEVNAME'], device['DEVTYPE'])


def list_partitions():
    for device in context.list_devices(subsystem='block', DEVTYPE='partition'):
        print '%s (label=%s, fs=%s) is located on %s' % (
            device.device_node,
            device.get('ID_FS_LABEL'),
            device.get('ID_FS_TYPE'),
            device.find_parent('block').device_node,
        )


from pprint import pprint as pp


'''
Example events:

> Creating a ZVOL:

event: add - /dev/zd64 - disk
{u'ACTION': u'add',
 u'DEVLINKS': u'/dev/dpool/whoavol /dev/zvol/dpool/whoavol',
 u'DEVNAME': u'/dev/zd64',
 u'DEVPATH': u'/devices/virtual/block/zd64',
 u'DEVTYPE': u'disk',
 u'MAJOR': u'230',
 u'MINOR': u'64',
 u'SEQNUM': u'2239',
 u'SUBSYSTEM': u'block',
 u'UDEV_LOG': u'3',
 u'UDISKS_PRESENTATION_NOPOLICY': u'1',
 u'USEC_INITIALIZED': u'16616516457'}
^ new disk

> Removing a ZVOL:

event: remove - /dev/zd64 - disk
{u'ACTION': u'remove',
 u'DEVLINKS': u'/dev/dpool/whoavol /dev/zvol/dpool/whoavol',
 u'DEVNAME': u'/dev/zd64',
 u'DEVPATH': u'/devices/virtual/block/zd64',
 u'DEVTYPE': u'disk',
 u'MAJOR': u'230',
 u'MINOR': u'64',
 u'SEQNUM': u'2241',
 u'SUBSYSTEM': u'block',
 u'UDEV_LOG': u'3',
 u'UDISKS_PRESENTATION_NOPOLICY': u'1',
 u'USEC_INITIALIZED': u'16616516457'}
^ removed disk

'''

def monitor_events():
    #monitor = pyudev.Monitor.from_netlink(context, source='kernel')
    monitor = pyudev.Monitor.from_netlink(context)  # Same as source='udev'

    # Filter incoming events
    #monitor.filter_by('block')

    def on_event(action, device):
        print
        print 'event: %s - %s - %s' % (action, device.device_node, device.device_type)
        pp(dict(device.items()))

        dev_type = device.get('DEVTYPE')

        if dev_type == 'disk':
            if action == 'add':
                print '^ new disk'
            elif action == 'remove':
                print '^ removed disk'
            else:
                print '^ disk'
        elif dev_type == 'partition':
            if action == 'add':
                print '^ new partition'
            elif action == 'remove':
                print '^ removed partition'
            else:
                print '^ partition'

        if 'ID_FS_TYPE' in device:
            print '^ label: {1}' % device.get('ID_FS_LABEL')

    """ async """
    observer = pyudev.MonitorObserver(monitor, on_event)
    observer.start()

    def sleep_till_intr():
        try:
            while True:
                time.sleep(1000)
        except (KeyboardInterrupt, SystemExit):
            raise
    sleep_till_intr()

    #""" sync (broken for some reason) """
    #for device in iter(monitor.poll, None):
    #    on_event(action, device)
    #    #on_event(device.action, device)


def main():
    logging.info('Listing block subsystem devices..')
    list_block_devices()

    logging.info('Listing partitions..')
    list_partitions()

    logging.info('Monitoring events..')
    monitor_events()


if __name__ == '__main__':
    main()
