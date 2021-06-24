import daemon
import signal
import time
import logging
import sys
import os
import datetime
import Data.Utils.pgdirectory as directory
from Daemon.Core import pidfile
from Daemon.Core.Type.watcher import watcherm
from Daemon.Core.Type.worker import workerm
from Daemon.Core.Type.sweeper import sweeperm
from Daemon.Core.Type.data_profiler import dataprofilem
from Daemon.Conf import dnconf
from Daemon.Core.Type.job_dispatcher import job_dispatcherm
from Daemon.Core import daemon_decorator as db
from Daemon.Model import Register
from Data.Config import pgconfig


__version__ = 0.1

loggingLevel = logging.INFO
logging.basicConfig(level=loggingLevel)

daemon_type_selector = {
    'worker': workerm,
    'watcher': watcherm,
    'jobdispatcher': job_dispatcherm,
    'sweeper': sweeperm,
    'dataprofiler': dataprofilem
}


def get_query(mdb, input_query_string):
    query_string = mdb.select(input_query_string)

    try:
        query = query_string[0][0]

    except IndexError:
        print(f"The given query {input_query_string} has a problem")
        return None

    return query


def program_cleanup(daemon_context):
    daemon_context.terminate
    logging.shutdown()


def reload_program_config():
    pass


@db.connect('meta')
def register(args, db_instance=None):

    try:

        db_instance.session.add(Register(args.daemon_id,
                             args.daemon_name,
                             args.daemon_type,
                             getpid(args),
                             'active'))
        db_instance.session.commit()

    except Exception as err:
        db_instance.session.query(Register).filter(Register.daemon_id == args.daemon_id).delete()
        db_instance.session.commit()
        db_instance.session.add(Register(args.daemon_id,
                                         args.daemon_name,
                                         args.daemon_type,
                                         getpid(args),
                                         'active'))
        db_instance.session.commit()
        args.logger.info(f"Error occurred while register the worker in the metadata! {err}")
    #finally:
    #    db_instance.close()


def main_routine(args):
    register(args)
    #args.conf = pgconfig.Config()
    args.conf = dnconf.DNConf()

    while True:
        try:
            args.logger.info("Retrieving table current info:")

            daemon_select(args.daemon_type, args)
            #DBPostgres(args)
            #mdb = DBPostgres(args)
            #mdb.close()

            current_time = datetime.datetime.now()
            args.logger.info(f"Task scheduled run time: {current_time}")

        except Exception as err:
            args.logger.info(f"Error Occurred while running the logic: {err}")

        args.logger.info(f"sleep a bit ({args.polling_interval})")
        time.sleep(args.polling_interval)


def process_request(args):
    if args.request == 'start':
        print("Starting Daemon...")
        daemon_start(args)
    elif args.request == 'stop':
        daemon_stop(args)
    elif args.request == 'status':
        daemon_status(args)
    elif args.request == 'restart':
        if daemon_status(args):
            daemon_stop(args)
        daemon_start(args)
    else:
        raise Exception("This request type is not found!")


def send_sig(tgt_pid, sig, msg):
    if tgt_pid == 0:
        logging.info("No processes running at this time")
        return False

    try:
        os.kill(tgt_pid, sig)
        logging.info(msg)
        return True

    except Exception:
        logging.info(f"Process {tgt_pid} is not running.")
        return False


def daemon_stop(args):
    msg = 'Process was asked to terminate'
    unregister(args)
    return send_sig(args.pid, 1, msg)


def daemon_status(args):
    msg = 'Process is running'
    return send_sig(args.pid, 0, msg)


def daemon_start(args):
    my_cwd = directory.currentdirectory()
    my_pidfile = args.my_pidfile
    #print(str(my_cwd))
    #print(my_pidfile)

    daemon_context = daemon.DaemonContext(
        working_directory=my_cwd,
        umask=0o002,
        pidfile=pidfile.PidFile(my_pidfile),
        stdout=sys.stdout,
        stderr=sys.stderr
    )

    daemon_context.signal_map = {
        signal.SIGTERM: program_cleanup(daemon_context),
        signal.SIGHUP: 'terminate',
        signal.SIGUSR1: reload_program_config,
    }
    try:
        with daemon_context:
            main_routine(args)
    except Exception as err:
        logging.critical(f"ERROR daemonizing: {err}")
        sys.exit(400)


def getpid(args):
    with open(args.my_pidfile, 'r') as file:
        data = file.read()
    return data


def daemon_select(daemon_type, args):

    func = daemon_type_selector.get(daemon_type, "not available")
    return func(args)


@db.connect('meta')
def unregister(args, db_instance=None):
    try:

        db_instance.session.query(Register).filter(Register.daemon_id == args.daemon_id).delete()
        db_instance.session.commit()

    except Exception as err:
        args.logger.info(f"Error occurred while register the worker in the metadata! {err}")

    #finally:
    #    db_instance.close()

