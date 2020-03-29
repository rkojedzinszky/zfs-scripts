""" pipemeter module """

import subprocess

CHUNKSIZE = 65536

def pipemeter(cmd1, cmd2):
    """ Effectively runs  cmd1 < /dev/null 2>/dev/null | cmd2 >/dev/null 2>/dev/null
    while counting bytes passed.

    Returns cmd1_return, cmd2_return, bytes_piped
    """

    proc1 = subprocess.Popen(cmd1, bufsize=0, shell=True, stdout=subprocess.PIPE)
    proc2 = subprocess.Popen(cmd2, bufsize=0, shell=True, stdin=subprocess.PIPE)
    bytes_piped = 0

    while True:
        data = proc1.stdout.read(CHUNKSIZE)
        length = len(data)
        if length == 0:
            break

        written = proc2.stdin.write(data)
        if written != length:
            raise RuntimeError("Write failed, wanted to write: {}, written={}".format(length, written))

        bytes_piped += length

    proc1.stdout.close()
    proc2.stdin.close()

    return proc1.wait(), proc2.wait(), bytes_piped
