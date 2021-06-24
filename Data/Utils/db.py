import contextlib
from functools import wraps
from Data.Connect import postgresql
from Data.Connect import redshift
from collections import namedtuple
from sqlalchemy.exc import DatabaseError, DataError, IntegrityError
import boto3
from Data.Config import pgconfig
import psycopg2

"""
@contextlib.contextmanager
def create_session(args):

    postgresql.DBPostgres(args)
    session = postgresql.Session()

    try:
        yield session
        session.expunge_all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def provide_session(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = 'session'

        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session(args) as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)

    return wrapper
"""

def validate_table_exist(table_list):
    return table_list


@contextlib.contextmanager
def create_session(db_type):

    conf = pgconfig.Config()

    action = {"rds":            postgresql.ConnectPostgresql1,
              "meta":           postgresql.ConnectPostgresql1,
              "redshift":       redshift.ConnectRedshift,

              }

    parameter = {"rds":         conf.setup_rds,
                 "meta":        conf.setup_meta,
                 "redshift":    conf.setup_redshift,
                 }

    instance = None
    try:

        instance = action[db_type](parameter[db_type]())
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

"""

def connect_rds(func):

    @wraps(func)
    def wrapper(*args, **kwargs):

        arg_session = 'db_instance'
        func_params = func.__code__.co_varnames

        session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session('rds') as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)

    return wrapper

"""

"""
Supported db_type is meta for metadata db, redshift for target data warehouse and rds for OLTP db

"""


def connect(db_type):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = 'db_instance'
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                with create_session(db_type) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)

        return wrapper
    return decorator


@contextlib.contextmanager
def create_session1(args):

    post = postgresql.ConnectPostgresql1(args)
    session = post.session

    try:
        yield session
        session.expunge_all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def get_db_parameter(db_type):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            args_session = 'db_parameter'
            func_params = func.__code__.co_varnames

            session_in_args = args_session in func_params and func_params.index(args_session) < len(args)
            session_in_kwargs = args_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                kwargs[args_session] = db_type
                return func(*args, **kwargs)
        return wrapper
    return decorator


def provide_session(func):

    @wraps(func)
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


def ping(db_parameter=None):
    try:
        conf = pgconfig.Config().get_parameter(db_parameter)

        conn=psycopg2.connect(dbname=conf.system_db,
                              host=conf.system_host,
                              port=conf.system_port,
                              user=conf.system_username,
                              password=conf.system_userpass)

        cur = conn.cursor()
        cur.execute("select count(1) from pg_database;")
        cur.fetchall()
        cur.close()
        conn.close()
        print("Successfully connected to database")
        return True
    except Exception as err:
        return False


def setup_rds():
    conf = pgconfig.Config()
    post_host = getattr(conf, 'SCOOT_RDS_POST_HOST')
    post_username = getattr(conf, 'SCOOT_RDS_POST_USERNAME')
    post_userpass = getattr(conf, 'SCOOT_RDS_POST_PASS')
    post_port = getattr(conf, 'SCOOT_RDS_POST_PORT')
    post_db = getattr(conf, 'SCOOT_RDS_POST_DB')

    Postgresql = namedtuple('Postgresql', ['post_host',
                                           'post_username',
                                           'post_userpass',
                                           'post_port',
                                           'post_db'])

    return Postgresql(post_host=post_host,
                      post_username=post_username,
                      post_userpass=post_userpass,
                      post_port=post_port,
                      post_db=post_db)


def ping_all(func_list):

    #func_list = []
    #func_list.append(get_db_parameter('redshift')(ping))
    #func_list.append(get_db_parameter('meta')(ping))

    return list(map(lambda x: x(), func_list))


if __name__ == '__main__':

    ping_all()
    exit(0)


    conf = pgconfig.Config()
    print(getattr(conf, 'SCOOT_S3_BUCKET_ACCESS_KEY'))
    print(getattr(conf, 'SCOOT_S3_BUCKET_SECRET_KEY'))

    client = boto3.client('redshift', region_name='us-west-1')
    response = client.describe_clusters(
        ClusterIdentifier='scoot-dw',
        MaxRecords=25
    )

    print(response)
