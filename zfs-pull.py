#!/usr/bin/env python3

import argparse
import subprocess
import syslog
import fcntl
import os
import time
from pipemeter import pipemeter

compress = {
        'lz4':  ['lz4 -c', 'lz4 -dc'],
        'gzip': ['gzip -c', 'gzip -dc'],
        'xz':   ['xz -c', 'xz -dc'],
        }

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
COMPRESS = None

def zfs_pull(srchost, srcds, dstds, fromsnap, tosnap):
    if fromsnap is None:
        message = 'Pulling snapshot for {}/{}: {} to {}'.format(srchost, srcds, tosnap, dstds)
        sendcmd = 'zfs send -R {}@{}'.format(srcds, tosnap)
    else:
        message = 'Pulling snapshot for {}/{}: ({}, {}] to {}'.format(srchost, srcds, fromsnap, tosnap, dstds)
        sendcmd = 'zfs send -I {} {}@{}'.format(fromsnap, srcds, tosnap)

    comp, decomp = "", ""
    if COMPRESS:
        comp = " | {}".format(COMPRESS[0])
        decomp = "{} |".format(COMPRESS[1])

    send = '{} {} "{}{}"'.format(SSH_CMD, srchost, sendcmd, comp)
    recv = '{}zfs receive -o readonly=on -x mountpoint -F -d {}'.format(decomp, dstds)

    start = time.time()
    syslog.syslog(syslog.LOG_INFO, message)
    send_ret, recv_ret, bytes_total = pipemeter(send, recv)
    if send_ret != 0:
        raise RuntimeError("Error running {}".format(send))
    if recv_ret != 0:
        raise RuntimeError("Error running {}".format(recv))
    end = time.time()
    syslog.syslog(syslog.LOG_INFO, '{} FINISHED IN {:.2f} seconds, {:d} bytes transferred'.format(message, end - start, bytes_total))

def read_snapshots(dataset):
    return [snap for snap in subprocess.check_output(['zfs list -t snapshot -S creation -d 1 -H -o name {} | sed -e "s/.*@//"'.format(dataset)], shell=True).decode().split('\n') if snap != '']

def read_remote_snapshots(host, dataset):
    return [snap for snap in subprocess.check_output(['{} {} "zfs list -t snapshot -S creation -d 1 -H -o name {} | sed -e \\"s/.*@//\\""'.format(SSH_CMD, host, dataset)], shell=True).decode().split('\n') if snap != '']

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

    compress_method = os.getenv("ZFS_PULL_COMPRESS", None)
    if compress_method:
        COMPRESS = compress.get(compress_method, None)
        if COMPRESS is None:
            raise RuntimeError("Invalid compress method: {}".format(compress_method))

    ldataset = sys.argv[3]

    with FileLock('/var/run/zfs-pull-{}.lock'.format(ldataset.replace('/', '--'))):
        main(*sys.argv[1:4])
