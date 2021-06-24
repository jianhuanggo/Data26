#metaclass=pgoperation.myMetaClass
from Data.Utils import pgoperation
from abc import ABCMeta


class A(metaclass=pgoperation.PGMetaClass):
    def __init__(self):
        pass


class B():
    def __init__(self):
        pass


class C(A, B):
    def x(self):
        print(1)


if __name__ == '__main__':
    test1 = C()
