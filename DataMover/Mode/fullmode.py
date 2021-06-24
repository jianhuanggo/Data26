import sys
from DataMover.Mode import basemode
from Data.Utils.check import check_test_table
from Data.Utils import db
from functools import wraps
from subprocess import Popen, PIPE, call, STDOUT
from Data.Config import pgconfig
from Data.Connect import redshift





@db.connect('redshift')
def target_apply_full(func, db_instance=None):
    @wraps(func)
    def wrapper(*args, **kwargs):

        arg_parameter = 'tablename'
        func_params = func.__code__.co_varnames

        session_in_args = arg_parameter in func_params and func_params.index(arg_parameter) < len(args)
        session_in_kwargs = arg_parameter in kwargs

        if session_in_kwargs or session_in_args:
            delete_query = f"delete from {kwargs[arg_parameter]}"
            print(delete_query)
            try:
                db_instance.session.execute(delete_query)
                db_instance.session.commit()
            except Exception as err:
                print(f"Could not delete {kwargs[arg_parameter]} table {err}")
                raise
            else:
                print(f"{kwargs[arg_parameter]} is deleted")
        else:
            raise ("Argument 'tablename' is not found.  Pls check again")

        return func(*args, **kwargs)
    return wrapper




def target_apply_full1(parameters):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            arg_parameter = 'tablename'
            func_params = func.__code__.co_varnames

            session_in_args = arg_parameter in func_params and func_params.index(arg_parameter) < len(args)
            session_in_kwargs = arg_parameter in kwargs

            if session_in_kwargs or session_in_args:
                delete_query = f"delete from {kwargs[arg_parameter]}"
                print(delete_query)

                try:
                    db_client_dbshell = parameters.db_client_dbshell.strip()
                    loadConf = [db_client_dbshell, parameters.redshift_url, "-c", delete_query]

                    p2 = Popen(loadConf)
                    p2.wait()
                except Exception as err:
                    print(f"Could not delete {kwargs[arg_parameter]} table {err}")
                    raise
                else:
                    print(f"{kwargs[arg_parameter]} is deleted")
            else:
                raise ("Argument 'tablename' is not found.  Pls check again")

            return func(*args, **kwargs)

        return wrapper
    return decorator


class FullMode(basemode.BaseMode):

    def __init__(self):
        pass

    def get_mode(self):
        pass

    @check_test_table
    def source_apply(self, tablename=None, db_instance=None):
        pass


    #@target_apply_full
    #@check_test_table
    def execute_mode(self, tablename=None):
        print(tablename)


    @check_test_table
    def target_apply(self, tablename=None, db_instance=None):
        if tablename:
            try:
                db_instance.session.execute(f"delete from {tablename}")
                db_instance.session.commit()

            except Exception as err:
                raise(f"Could not delete records in the table {err}")
                sys.exit(300)

            finally:
                db_instance.close()

    #target_apply = db.connect('redshift')(target_apply)


if __name__ == '__main__':
    fullmode = FullMode()
    fullmode.execute_mode(tablename="test_me")



