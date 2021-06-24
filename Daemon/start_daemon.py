import os
import sys
import time
import logging
import argparse
import subprocess
sys.path.append('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20')
from Daemon.Core import daemon_decorator as db
from Daemon.Model import Register
from Daemon.Conf import dnconf

from Meta.pggenericfunc import notimplemented


from Daemon.Core import daemon
import Data.Utils.StrFunc as StrFunc
import socket

from sqlalchemy import select
from sqlalchemy import func
from sqlalchemy import Sequence

_version_ = 1.0

DEFAULT_PYTHON_PATH = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20"
daemon_start_point = { 'jobdispatcher': 2000,
                        'watcher': 5000,
                        'worker': 10000,
}


def nextval_mysql(seq: str) -> list:
    return [f"update metaschema.scd_register_daemon_id_seq set seq_id = LAST_INSERT_ID (seq_id + 1)",
            f"select LAST_INSERT_ID()"]


def nextval_oracle(seq: str) -> list:
    return [f"select nextval('{seq}')"]


def nextval_postresql(seq: str) -> list:
    return [f"select nextval('{seq}')"]


def get_parser() -> argparse.Namespace:
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='Daemon Framework - start daemon script')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-y', '--worker_type', action='store', dest='daemon_type', required=True, help='Type of Daemon')
        argparser.add_argument('-n', '--num_daemon', action='store', dest='num_daemon', type=int, required=True, help='Number of Daemons')
        argparser.add_argument('-a', '--all_flag', action='store', dest='all_flag', type=bool, help='Number of Daemons')
        argparser.add_argument('request', action='store', choices=['startup', 'shutdown'],
                               help='start or stop number of daemons')
        logging.debug('parsing argparser arguments')

        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    # print(args.daemon_type)
    return args


@db.connect('meta')
def register_daemon(*, daemon_type: str, num_daemon: str, db_instance=None) -> bool:

    """
    For Oracle, it is a sequence object
    For Mysql, it is a table with an autoincrement column
    For Postreqsql, it is a sequence object - CREATE SEQUENCE scd_register_daemon_id_seq START 1;
    """
    seq = "metaschema.scd_register_daemon_id_seq"

    get_seq = {'mysql': nextval_mysql,
               'oracle': nextval_oracle,
               'postresql': nextval_postresql
               }

    for id_offset in range(int(num_daemon)):
        try:
            for query_item in get_seq[dnconf.DNConf().parameters['meta_type']](seq):
                _result = db_instance.session.execute(query_item)
                db_instance.session.commit()
            get_daemon_id = int(_result.fetchall()[0][0])
        except Exception as err:
            raise err

        #get_daemon_id = int(db_instance.session.execute(f"select nextval('{seq}')").fetchall()[0][0])
        daemon_id = int(daemon_start_point[daemon_type]) + int(get_daemon_id)
        command = f"python pgdaemon.py -i {daemon_id} -t 10 -y {daemon_type} start"
        #print(command)

        loadConf = ['python', 'pgdaemon.py', "-i", str(daemon_id), "-t",  "60", "-y",  daemon_type, "start"]
        #print(loadConf)
        # self.args.logger.debug(loadConf)

        p2 = subprocess.Popen(loadConf)
        p2.wait()
        if p2.returncode != 0:
            return False
        time.sleep(2)

    return True


def shutdown_daemon(*, daemon_type: str, daemon_list: list) -> bool:

    for daemon_id in daemon_list:
        #command = f"python pgdaemon.py -i {daemon_id} -y {daemon_type} stop"
        loadConf = ['python', 'pgdaemon.py', "-i", str(daemon_id), "-y",  daemon_type, "stop"]
        print(loadConf)

        p2 = subprocess.Popen(loadConf)
        p2.wait()
        if p2.returncode != 0:
            return False
        time.sleep(2)

    return True


@db.connect('meta')
def get_daemon_list(*, daemon_type: str, num_daemon: str, all_flag: bool = False, db_instance=None) -> list:
    daemon_id_list = []
    query = db_instance.session.query(Register).filter(Register.daemon_type == daemon_type)
    for _row in query.all():
        daemon_id_list.append(_row.daemon_id)
        if not all_flag:
            num_daemon -= 1
            if num_daemon == 0:
                break

    return daemon_id_list


@db.connect('meta')
def mysql_create_seq_table(table_name: str, db_instance=None) -> bool:
    seq_creation_sql = [f"create table {table_name} (seq_id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (seq_id))",
                        f"insert into {table_name} VALUES (0)"]
    try:
        list(map(lambda x: db_instance.session.execute(x), seq_creation_sql))
    except Exception as err:
        raise err
    return True


if __name__ == '__main__':
    #mysql_create_seq_table("metaschema.scd_register_daemon_id_seq")

    args = get_parser()
    conf = dnconf.DNConf()

    if "PYTHONPATH" not in os.environ:
        if "PYTHONPATH" in conf.parameters:
            os.environ['PYTHONPATH'] = conf.parameters['PYTHONPATH']
        else:
            os.environ['PYTHONPATH'] = DEFAULT_PYTHON_PATH

    if args.request == 'shutdown':
        daemon_list = get_daemon_list(daemon_type=args.daemon_type, num_daemon=int(args.num_daemon), all_flag=args.all_flag)
        if shutdown_daemon(daemon_type=args.daemon_type, daemon_list=daemon_list):
            print(f"Total {str(args.num_daemon)} have been shutdown successfully!")
    elif args.request == 'startup':
        if register_daemon(daemon_type=args.daemon_type, num_daemon=args.num_daemon):
            print(f"Total {str(args.num_daemon)} have been started successfully!")

