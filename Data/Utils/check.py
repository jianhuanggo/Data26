from functools import wraps


def check_test_table(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        arg_session = 'tablename'
        func_params = func.__code__.co_varnames

        session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            if '_' in kwargs[arg_session] and ('test' != str(kwargs[arg_session].split('_')[0])):
                raise ("This is not a test table, Please check again.  Remove check_test_table decorator for production run")
            else:
                return func(*args, **kwargs)
        else:
            raise ("Tablename argument is not found.  Pls check again")

    return wrapper


def check_const_list(content_list: list, blacklist_item: list, msg: str) ->list:
    for item in blacklist_item:
        if item in content_list:
            raise msg

    return content_list




