from reprlib import recursive_repr
from makefun import wraps

import inspect
import functools
from API.RealEstate import pgrealestate

class partial2(object):

    def __init__(*args, **kw):
        self = args[0]
        self.fn, self.args, self.kw = (args[1], args[2:], kw)

    def __call__(self, *args, **kw):
        if kw and self.kw:
            d = self.kw.copy()
            d.update(kw)
        else:
            d = kw or self.kw
        return self.fn(*(self.args + args), **d)

class partial1:
    """
    partial(func, *args, **keywords) - new function with partial application
    of the given arguments and keywords.
    """
    __slots__ = ['func', 'args', 'keywords']

    def __init__(self, func, *args, **keywords):
        if not callable(func):
            raise TypeError("the first argument must be callable")
        self.func = func
        self.args = args
        self.keywords = keywords

    def __call__(self, *fargs, **fkeywords):
        newkeywords = self.keywords.copy()
        newkeywords.update(fkeywords)
        return self.func(*(self.args + fargs), **newkeywords)





class partial:
    """New function with partial application of the given arguments
    and keywords.
    """

    __slots__ = "func", "args", "keywords", "__dict__", "__weakref__"

    def __new__(cls, func,  *args, **keywords):
        if not callable(func):
            raise TypeError("the first argument must be callable")

        if hasattr(func, "func"):
            args = func.args + args
            keywords = {**func.keywords, **keywords}
            func = func.func

        self = super(partial, cls).__new__(cls)

        self.func = func

        self.args = args
        print(args)
        self.keywords = keywords
        self.func = func
        print(self.func)

        return self

    def __call__(self,  *args, **keywords):
        print(f"ok11: {inspect.signature(self.func)}")
        keywords = {**self.keywords, **keywords}
        return self.func(*self.args, *args, **keywords)

        #return self.func(a=5, b=7)

    @recursive_repr()
    def __repr__(self):
        qualname = type(self).__qualname__
        args = [repr(self.func)]
        args.extend(repr(x) for x in self.args)
        args.extend(f"{k}={v!r}" for (k, v) in self.keywords.items())
        if type(self).__module__ == "functools":
            return f"functools.{qualname}({', '.join(args)})"
        return f"{qualname}({', '.join(args)})"

    def __reduce__(self):
        return type(self), (self.func,), (self.func, self.args,
               self.keywords or None, self.__dict__ or None)

    def __setstate__(self, state):
        if not isinstance(state, tuple):
            raise TypeError("argument to __setstate__ must be a tuple")
        if len(state) != 4:
            raise TypeError(f"expected 4 items in state, got {len(state)}")
        func, args, kwds, namespace = state
        if (not callable(func) or not isinstance(args, tuple) or
           (kwds is not None and not isinstance(kwds, dict)) or
           (namespace is not None and not isinstance(namespace, dict))):
            raise TypeError("invalid partial state")

        args = tuple(args) # just in case it's a subclass
        if kwds is None:
            kwds = {}
        elif type(kwds) is not dict: # XXX does it need to be *exactly* dict?
            kwds = dict(kwds)
        if namespace is None:
            namespace = {}

        self.__dict__ = namespace
        self.func = func
        self.args = args
        self.keywords = kwds

"""
try:
    from _functools import partial

    print(dir(partial.__qualname__))
except ImportError:
    print("something is wrong")
"""


#@pgrealestate.api_real_estate("redfin")
def test(a, b):
    print(locals())
    return a + b



def decorator(func):
    #@functools.wraps()
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper




if __name__ == '__main__':
    print(dir(test))
    #aa = pgrealestate.api_real_estate("redfin")(test)
    #print(aa(2, 5))
    #print(inspect.signature(aa))
    bb = decorator(test)

    print(inspect.signature(bb))
    bb(5, 10)
    """
    tea = partial(test)
    tan = functools.partial(test)
    ten = decorator(test)
    print(dir(tan.__dict__))
    print(dir(test.__call__))

    print(ten(2, 7))
    #print(tea(2, 5))
    #tail = partial1(test)
    #tall = partial2(test)
    print(inspect.signature(test))
    print(inspect.signature(tea))
    print(inspect.signature(ten))
    #print(inspect.signature(tan))
    #print(inspect.signature(tail))
    """



