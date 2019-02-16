# zfs-scripts

These are small scripts for expiring and snapshot replication.

## zfs-expire-snapshot.py

This script uses the pattern @auto-YYYYMMDD[-]?HHMM to match snapshots, and expires (removes) them according to
command line specifications (or defaults).

It assigns a type for each snapshot (hourly, daily, weekly), and based on specifications, it decides to keep or
expire the snapshot. If at least one type specification requires the snapshot to be kept, it will be kept.

## zfs-pull.py

Pulls the latest snapshot from the remote site to local site, using incremental sends, or for the first time using a
full replication strem.
