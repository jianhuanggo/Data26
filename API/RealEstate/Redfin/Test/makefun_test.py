from makefun import wraps
import inspect
from API.RealEstate import pgrealestate


#@pgrealestate.api_real_estate("redfin")




def decorator(func):
    func_sig = inspect.signature(func)
    print(f"Original Signature: {func_sig}")

    params = list(func_sig.parameters.values())
    print(params)
    params.insert(0, inspect.Parameter('z', kind=inspect.Parameter.POSITIONAL_OR_KEYWORD))
    new_sig = func_sig.replace(parameters=params)

    @wraps(func, new_sig=new_sig)
    def wrapper(z, *args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@pgrealestate.api_real_estate(object_type="another_redfin", object_name="notok")
@pgrealestate.api_real_estate(object_type="redfin", object_name="myredfin")
@pgrealestate.api_real_estate(object_type="redfin", object_name="myredfin")
def test(a, b, _pg_action=None):
    print(_pg_action.redfin.myredfin)
    #print(_pg_action.get("redfin"))
    return a + b


if __name__ == '__main__':
    #mytest = pgrealestate.api_real_estate("another_redfin")(test)

    test(2, 5)

    print(inspect.signature(test))



