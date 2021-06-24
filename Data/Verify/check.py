#!/usr/bin/python

import os
import sys
import Data.Utils.pgdirectory as directory


def checklogging():
    cwd = directory.currentdirectory()
    if not directory.isdirectoryexist(os.path.join(cwd + '/logs/daemon_log')):
        print(f"Missing log directory {os.path.join(cwd + '/logs/daemon_log')}")
        sys.exit(100)

    if not directory.isdirectoryexist(os.path.join(cwd + '/logs/standard_log')):
        print(f"Missing log directory {os.path.join(cwd + '/logs/standard_log')}")
        sys.exit(100)


def checkpid():
    cwd = directory.currentdirectory()
    if not directory.isdirectoryexist(os.path.join(cwd + '/pid')):
        print(f"Missing log directory {os.path.join(cwd + '/pid')}")
        sys.exit(100)
