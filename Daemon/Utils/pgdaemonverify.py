#!/usr/bin/python

import os
import sys
import Data.Utils.pgdirectory as directory
from Daemon.Conf import dnconf as pgconf


def checklogdir(conf: pgconf.DNConf):
    cwd = conf.parameters[f"{conf.parameters['ENV_VAR_PREFIX']}DATA_HOME"]

    if not directory.isdirectoryexist(os.path.join(cwd + '/logs/daemon_log')):
        print(f"Missing log directory {os.path.join(cwd + '/logs/daemon_log')}")
        try:
            directory.createdirectory(f"{os.path.join(cwd + '/logs/daemon_log')}")
        except Exception as err:
            raise err
        finally:
            print(f"{os.path.join(cwd + '/logs/daemon_log')} is created")

    if not directory.isdirectoryexist(os.path.join(cwd + '/logs/standard_log')):
        print(f"Missing log directory {os.path.join(cwd + '/logs/standard_log')}")
        try:
            directory.createdirectory(f"{os.path.join(cwd + '/logs/standard_log')}")
        except Exception as err:
            raise err
        finally:
            print(f"{os.path.join(cwd + '/logs/standard_log')} is created")


def checkpiddir(conf: pgconf.DNConf):
    cwd = conf.parameters[f"{conf.parameters['ENV_VAR_PREFIX']}DATA_HOME"]

    if not directory.isdirectoryexist(os.path.join(cwd + '/pid')):
        print(f"Missing log directory {os.path.join(cwd + '/pid')}")
        try:
            directory.createdirectory(f"{os.path.join(cwd + '/pid')}")
        except Exception as err:
            raise err
        finally:
            print(f"{os.path.join(cwd + '/pid')} is created")
