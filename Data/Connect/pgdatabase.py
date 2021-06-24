import inspect
import contextlib
import functools
from typing import Callable, Any, TypeVar
from Meta import pggenericfunc
from Data.Connect import postgresql
from Data.Connect import redshift
#from Data.Connect import pgmysql
from Data.Connect.Mysql import pgmysql
from sqlalchemy.exc import DatabaseError, DataError, IntegrityError
import boto3
from Data.Config import pgconfig

F = TypeVar('F', bound=Callable[..., Any])

@contextlib.contextmanager
def create_session(db_name):
    _conf = pgconfig.Config()

    action = {"rds":            postgresql.ConnectPostgresql,
              "meta":           postgresql.ConnectPostgresql,
              "redshift":       redshift.ConnectRedshift,
              "mysql":          pgmysql.PGMysqlLiteExt,
              }

    parameter = {"rds":         _conf.setup_db,
                 "meta":        _conf.setup_db,
                 "redshift":    _conf.setup_db,
                 "mysql":       _conf.setup_db}

    instance = None
    try:
        #instance = action[db_name](parameter[db_name](db_name))
        instance = action[db_name]()
        yield instance
        #instance.session.expunge_all()
        #instance.session.commit()
    except (DatabaseError, DataError, IntegrityError) as e:
        print(f"In the exception clause!!!")
        instance.session.rollback()
        raise (f"Something wrong with the session {e}")
    except BaseException as e:
        raise ('Wrong!!! %v'.format(e))
    #finally:
    #    instance.session.close()
    #    instance.engine.dispose()


def db_session(db_name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = 'db_session'
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
def pg_set_db(pg_object_type: str,
              pg_object_name: str = "default",
              pg_object_parameter: dict = None,
              pg_subscription_level=1,
              _logger=None):

    _conf = pgconfig.Config()

    action = {"mysql":        {'1': pgmysql.PGMysqlLite,
                                '999': pgmysql.PGMysqlLiteExt,
                               },
              "redshift":      {'1': postgresql.ConnectPostgresql,
                                '2': postgresql.ConnectPostgresql,
                                '999': postgresql.ConnectPostgresql,
                                },
              }

    parameter = {"mysql":     {'1': pg_object_parameter,
                               '2': pg_object_parameter,
                               '999': pg_object_parameter,
                                },
                 "redshift":   {'1': pg_object_parameter,
                                '2': pg_object_parameter,
                                '999': pg_object_parameter,
                                },
                 }

    try:
        if pg_object_type not in action:
            pggenericfunc.pg_error_logger(_logger,
                                          inspect.currentframe().f_code.co_name,
                                          f"{pg_object_type} not found, currently {', '.join(action.keys())} are supported")
            raise

        instance_session = action.get(pg_object_type, None)[str(pg_subscription_level)]()
        if instance_session:
            instance_session.set_param(**parameter.get(pg_object_type, None)[str(pg_subscription_level)])

        yield instance_session if instance_session else None

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger,
                                      inspect.currentframe().f_code.co_name,
                                      err)


def pg_db_session(pg_object_type: str,
                  pg_object_name: str = None,
                  pg_variable_name: str ="_pg_action",
                  pg_object_parameter: dict = None,
                  pg_subscription_level: int = 1) -> Callable[[F], F]:

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = pg_variable_name
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            #print(session_in_args)
            #print(session_in_kwargs)
            #print(kwargs)

            #if (session_in_kwargs or session_in_args) and variable_name in kwargs and object_type in kwargs[variable_name]
            #    if object_name is None or object_name in kwargs[variable_name][object_type]:

            if (session_in_kwargs or session_in_args) and pg_variable_name in kwargs and pg_object_type in kwargs[pg_variable_name]:
                if pg_object_name is None or pg_object_name in kwargs[pg_variable_name][pg_object_type]:
                    return func(*args, **kwargs)
            else:
                with pg_set_db(pg_object_type=pg_object_type,
                               pg_object_name=pg_object_name,
                               pg_object_parameter=pg_object_parameter,
                               pg_subscription_level=pg_subscription_level) as session:
                    #print(session)
                    if session:
                        _object_name = pg_object_name or "default"
                        if pg_variable_name in kwargs:
                            #kwargs[variable_name][f"storage_{object_type}_{_object_name}"] = {_object_name: session}
                            kwargs[pg_variable_name][f"db_{pg_object_type}_{_object_name}"] = session
                        else:
                            #object_name_namespace = {_object_name: session}
                            #kwargs[variable_name] = {f"storage_{object_type}_{_object_name}": object_name_namespace}
                            kwargs[pg_variable_name] = {f"db_{pg_object_type}_{_object_name}": session}

                    #print(kwargs)
                return func(*args, **kwargs)

        return wrapper
    return decorator
