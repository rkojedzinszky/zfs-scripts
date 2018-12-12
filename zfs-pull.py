#!/usr/bin/env python

from __future__ import print_function, unicode_literals
import argparse
import subprocess
import syslog
import fcntl
import os
import time

class FileLock(object):
    def __init__(self, lockfile):
        self.lockfile = lockfile
        self.fd = None

    def acquire(self):
        self.fd = os.open(self.lockfile, os.O_WRONLY | os.O_CREAT)
        fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def release(self):
        if self.fd:
            os.close(self.fd)
            self.fd = None

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

SSH_CMD = 'ssh -o BatchMode=yes'

def zfs_pull(srchost, srcds, dstds, fromsnap, tosnap):
    if fromsnap is None:
        message = 'Pulling snapshot for {}/{}: {} to {}'.format(srchost, srcds, tosnap, dstds)
        sendcmd = 'zfs send -R {}@{}'.format(srcds, tosnap)
    else:
        message = 'Pulling snapshot for {}/{}: ({}, {}] to {}'.format(srchost, srcds, fromsnap, tosnap, dstds)
        sendcmd = 'zfs send -I {} {}@{}'.format(fromsnap, srcds, tosnap)

    fullcmd = '{} {} "{}" | zfs receive -x mountpoint -F -d {}'.format(SSH_CMD, srchost, sendcmd, dstds)

    start = time.time()
    syslog.syslog(syslog.LOG_INFO, message)
    subprocess.check_call([fullcmd], shell=True)
    end = time.time()
    syslog.syslog(syslog.LOG_INFO, '{} FINISHED IN {:.2f} seconds'.format(message, end - start))

def read_snapshots(dataset):
    return [snap for snap in subprocess.check_output(['zfs list -t snapshot -S creation -d 1 -H -o name {} | sed -e "s/.*@//"'.format(dataset)], shell=True).split('\n') if snap != '']

def read_remote_snapshots(host, dataset):
    return [snap for snap in subprocess.check_output(['{} {} "zfs list -t snapshot -S creation -d 1 -H -o name {} | sed -e \\"s/.*@//\\""'.format(SSH_CMD, host, dataset)], shell=True).split('\n') if snap != '']

def main(srchost, srcds, dstds):

    try:
        pool, ds = srcds.split('/', 1)
    except:
        print ('Root dataset replication is not supported')
        sys.exit(1)

    remote_snapshots = read_remote_snapshots(srchost, srcds)
    local_snapshots = read_snapshots('{}/{}'.format(dstds, ds))

    fromsnap = None
    tosnap = remote_snapshots[0]

    local_snapshots = set(local_snapshots)
    for snap in remote_snapshots:
        if snap in local_snapshots:
            fromsnap = snap
            break

    if fromsnap == tosnap:
        syslog.syslog(syslog.LOG_INFO, 'Dataset {}/{} up-to-date'.format(srchost, srcds))
    else:
        zfs_pull(srchost, srcds, dstds, fromsnap, tosnap)

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 4:
        sys.stderr.write('Usage: {} <src host> <src ds> <dest ds>'.format(sys.argv[0]))
        sys.exit(1)

    ldataset = sys.argv[3]

    with FileLock('/var/run/zfs-pull-{}.lock'.format(ldataset.replace('/', '--'))):
        main(*sys.argv[1:4])
