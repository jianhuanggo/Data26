from abc import ABCMeta, abstractmethod

__version__ = "1.5"


class PGStorageFormatBase(metaclass=ABCMeta):

    @abstractmethod
    def inst(self):
        """ Implement in the Child Class """

    @abstractmethod
    def decode(self, data: object, *args, **kwargs):
        """ Implement in the Child Class """

    @abstractmethod
    def encode(self, data: object, *args, **kwargs):
        """ Implement in the Child Class """