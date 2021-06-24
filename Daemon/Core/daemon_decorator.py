"""
Exclusively for the purpose of managing daemon
"""

import contextlib
import functools
from Data.Connect import postgresql
from Data.Connect import redshift
from Data.Connect import pgmysql
from Daemon.Logging import pgdaemonlogging
from collections import namedtuple
from sqlalchemy.exc import DatabaseError, DataError, IntegrityError
import boto3
from Daemon.Conf import dnconf
import psycopg2

"""
mypackage = __import__('mypackage')
mymodule = getattr(mypackage, 'mymodule')
myfunction = getattr(mymodule, 'myfunction')

myfunction(parameter1, parameter2)

"""


@contextlib.contextmanager
def create_session(db_name):
    _conf = dnconf.DNConf("DAEMON_")

    action = {"rds":            postgresql.ConnectPostgresql,
              "meta":           postgresql.ConnectPostgresql,
              "redshift":       redshift.ConnectRedshift,
              "mysql":          pgmysql.ConnectMysql,
              }

    parameter = {"rds":         _conf.setup_db,
                 "meta":        _conf.setup_db,
                 "redshift":    _conf.setup_db,
                 "mysql":       _conf.setup_db,
                 }

    instance = None
    try:

        instance = action[db_name](parameter[db_name](db_name))
        yield instance
        instance.session.expunge_all()
        instance.session.commit()
    except (DatabaseError, DataError, IntegrityError) as e:
        print(f"In the exception clause!!!")
        instance.session.rollback()
        raise (f"Something wrong with the session {e}")
    except BaseException as e:
        raise ('Wrong!!! %v'.format(e))
    finally:
        instance.session.close()
        instance.engine.dispose()


def connect(db_name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = 'db_instance'
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                with create_session(db_name) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)

        return wrapper
    return decorator


@contextlib.contextmanager
def find_logger():
    try:
        pgdaemon_logger = pgdaemonlogging.PGDaemonLogging.get_instance()
        yield pgdaemon_logger

    except Exception as err:
        raise Exception(f"Something wrong logger {err}")


def get_logger():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            arg_session = 'pgdaemon_logger'
            func_params = func.__code__.co_varnames
            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                with find_logger() as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)
        return wrapper
    return decorator



def get_db_parameter(db_name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            args_session = 'db_parameter'
            func_params = func.__code__.co_varnames

            session_in_args = args_session in func_params and func_params.index(args_session) < len(args)
            session_in_kwargs = args_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                kwargs[args_session] = db_name
                return func(*args, **kwargs)
        return wrapper
    return decorator


def provide_session(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = 'session'

        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session1(args) as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)

    return wrapper


@connect('rds')
def get_column_name_by_order(tablename, db_instance=None):
    return db_instance.session.execute(f"select column_name, ordinal_position FROM information_schema.columns WHERE "
                                       f"table_name = '{tablename}' and table_schema = 'public'").fetchall()



if __name__ == '__main__':

    conf = pgconfig.Config()
    print(getattr(conf, 'SCOOT_S3_BUCKET_ACCESS_KEY'))
    print(getattr(conf, 'SCOOT_S3_BUCKET_SECRET_KEY'))

    client = boto3.client('redshift', region_name='us-west-1')
    response = client.describe_clusters(
        ClusterIdentifier='scoot-dw',
        MaxRecords=25
    )

    print(response)
