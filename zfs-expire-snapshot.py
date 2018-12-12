#!/usr/bin/env python

import datetime
import argparse
import re
import subprocess
import syslog

tsreg = re.compile('auto-(\d{4})(\d{2})(\d{2})-?(\d{2})(\d{2})')

def parse_snapshot(snapshot_name):
    """ Parse a snapshot, returns
    (timestamp, "hd?w?" string, which denotes if it is a
    hourly, daily, or weekly snapshot. Multiple chars may be present.
    """
    m = tsreg.match(snapshot_name)
    if m:
        year, month, day, hour, minute = m.groups()

        ts = datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minute))

        typ = 'h'
        if ts.hour == 0:
            typ += 'd'
            if ts.weekday() == 0:
                typ += 'w'

        return ts, typ

def expire_snapshots(dataset, recursive, expires):
    cmd = "/sbin/zfs list -t snapshot -H -o name -s name {} {}".format("-r" if recursive else "-d 1", dataset)
    for s in subprocess.check_output(cmd, shell=True).split('\n'):
        if s == '':
            break

        ds, snapshot = s.split('@')
        try:
            ts, typ = parse_snapshot(snapshot)
        except:
            continue

        keep = False
        for c in typ:
            if ts >= expires[c]:
                keep = True
                break

        if not keep:
            syslog.syslog(syslog.LOG_INFO, 'Destroying snapshot {}'.format(s))

            cmd = '/sbin/zfs destroy {}'.format(s)
            subprocess.check_call(cmd, shell=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Expire zfs snapshots')
    parser.add_argument('-H', '--hourly', default=168, help='Keep hourly snapshots for this many hours')
    parser.add_argument('-D', '--daily', default=14, help='Keep daily snapshots for this many days')
    parser.add_argument('-W', '--weekly', default=8, help='Keep weekly snapshots for this many weeks')
    parser.add_argument('-r', '--recursive', default=False, action='store_true', help='Use recursive processing of snapshots')
    parser.add_argument('datasets', nargs='+')
    args = parser.parse_args()

    now = datetime.datetime.now()

    expires = {
            'h': now - datetime.timedelta(hours=int(args.hourly)),
            'd': now - datetime.timedelta(days=int(args.daily)),
            'w': now - datetime.timedelta(weeks=int(args.weekly)),
            }

    for ds in args.datasets:
        expire_snapshots(ds, args.recursive, expires)
