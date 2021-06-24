import contextlib
import functools
from Data.Config import pgconfig
from Data.Connect import postgresql
from Data.Connect import redshift

"""
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
    except Exception as e:
        raise (f"Something wrong with the session {e}")
    finally:
        print("I'm done")


class PGDecorator:
    def __init__(self, func_dic, db_flag=False):
        self._func_dic = func_dic
        self._db_flag = db_flag

    def db_connect(self, func_type):
        """
        This decorator is intended for switching db type
        If the function already contains an active db_engine object, then this does nothing
        Otherwise, it will invoke db connect to obtain a db_engine object and assign to the function using variable db_instance

        """
        #if func_type not in self._func_dic:
        #    print(f"function type {func_type} is not registered")
        #    raise
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                arg_session = 'db_instance' if self._db_flag is True else func_type
                func_params = func.__code__.co_varnames

                session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
                session_in_kwargs = arg_session in kwargs

                if session_in_kwargs or session_in_args:
                    return func(*args, **kwargs)
                else:
                    with self._func_dic['db_instance'](func_type) as session:
                        kwargs[arg_session] = session
                        return func(*args, **kwargs)
            return wrapper
        return decorator

"""
Avoid disappearance of docstring when using function pointer
"""


def passmein(func):
    def wrapper(*args, **kwargs):
        return func(func, *args, **kwargs)
    return wrapper


@passmein
def my_func(me):
    print(me.__doc__)


def mytest(db_instance=None):
    if db_instance:
     print("db session")


if __name__ == '__main__':
    func_dict = {'db_instance': create_session}
    test = PGDecorator(func_dict, db_flag=True)

    mytest()
    test.db_connect('rds')(mytest)()

