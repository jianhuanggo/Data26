from typing import Generator
from Data.Utils import pgoperation


def pg_init_decorator() -> Generator:

    pg_dec_map = {"try_catch":    pgoperation.pg_try_catch,
                  }

    parameters = {"try_catch":      (1,),
                  }

    return ((val, parameters[key]) for key, val in pg_dec_map.items() if val)


class PGMetaClass(type):
    """
    A MetaClass to facilitate the creation of classes and apply common funnctionalities to
    their methods

    Attributes
    ----------

    Methods
    -------
    __new__(mcs, name, bases, local)
        Apply decorator pg_try_catch to the methods of classes which inherited this metaclass
    """
    def __new__(mcs, name, bases, local):
        """ Overrides the new method to apply a list of decorators to the class methods inherited or otherwise

        Args:
            name: name of the class
            bases: name of the base class(es)
            local: locals

        Returns:
             Returns a new class instance if successful

        """
        for _method in (attr for attr in local if callable(local[attr])):
            #local[_method] = pgoperation.pg_try_catch()(local[_method])
            for _dec in list(map(lambda x: x[0](*x[1]) if x[1] else x[0](), pg_init_decorator())): local[_method] = _dec(local[_method])
            #local[_method] = list(map(lambda x: x[0](*x[1])(local[_method]) if x[1] else x[0]()(local[_method]), pg_init_decorator()))[0]
        return type.__new__(mcs, name, bases, local)


class test100(metaclass=PGMetaClass):
    def __init__(self):
        print(1)

    def x(self):
        print(2)

    def y(self):
        print(3)


if __name__ == "__main__":
    def test(num: int = 1, stt: str = "ok"):
        print(num)
        print(stt)
    list(map(lambda x: x(), [test]))
    list(map(lambda x: x[0](test) if x[1] else x[0](*x[1])(test), pg_init_decorator()))
    print(pg_init_decorator())
    test100()
