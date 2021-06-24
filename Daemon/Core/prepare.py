import os
import sys
import logging
import Data.Utils.pgdirectory as directory
from Daemon.Utils import pgdaemonverify as check
from Daemon.Conf import dnconf
from Daemon.Logging import pgdaemonlogging
from Daemon.Model import base
from Daemon.Core import daemon_decorator as db


loggingLevel = logging.INFO
logging.basicConfig(level=loggingLevel)


@db.connect('meta')
def create_all_table(db_instance=None):
    try:
        base.Base.metadata.create_all(db_instance.engine)
    except Exception as err:
        raise (f"Can't create all tables. {err}")


def prepare(args):

    conf = dnconf.DNConf()
    conf.setup_daemon()

    if args.request == 'start':
        # check appropriate log directory exists
        check.checklogdir(conf)

        # check appropriate pid directory exists
        check.checkpiddir(conf)

    try:
        args.logger = pgdaemonlogging.PGDaemonLogging(conf, logging_level=loggingLevel,
                              subject='{0} logger'.format(args.daemon_name)).getLogger(args.daemon_name)

    except Exception as err:
        logging.critical(f"unable to instantiate Daemon logger {err}")
        sys.exit(300)

    args.logger.debug('Instantiate config and metadata objects')
    args.logger.info(f"The log for daemon '{args.daemon_name}' is stored at {getattr(conf, 'logger_file_filedir')}")

    args.my_pidfile = os.path.join(directory.currentdirectory() + '/pid/' + args.daemon_name + '.pid')
    args.logger.debug(f"args.my_pidfile: {args.my_pidfile}")

    try:
        args.pid = int(open(args.my_pidfile, 'r').read())

    except Exception as err:
        args.pid = 0
        #raise (f"Could not get pid {err}")

    args.logger.debug(f"args.pid: {str(args.pid)}")

    create_all_table()

    return args
