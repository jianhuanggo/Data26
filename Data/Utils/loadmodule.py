import sys
import os

def loadmodule(homedirectory, subdirectory, logger=None):
    '''
    :param homedirectory: program homework directory
    :param subdirectory: a list of subdirectories which needs to be imported
    :return:
    '''

    try:
        for dir in subdirectory:
            sys.path.insert(0, os.path.join(homedirectory, dir))
            if logger:
                logger.info(f"bootstrapping load path {os.path.join(homedirectory, dir)}")

    except Exception as err:
        if logger:
            logger.critical('unable to bootstrap loadpath for required modules:\n{0}'.format(str(err)))
        sys.exit(200)
