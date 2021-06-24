import os
from Daemon.Core import cli, daemon, prepare as pre
#import resource
#import sys


# Will segfault without this line.
#resource.setrlimit(resource.RLIMIT_STACK, [0x10000000, resource.RLIM_INFINITY])
#sys.setrecursionlimit(0x100000)

__version__ = 0.1

if __name__ == '__main__':

    args = cli.get_parser()
    args = pre.prepare(args)
    daemon.process_request(args)


