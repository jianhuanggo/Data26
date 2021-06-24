from abc import ABCMeta, abstractmethod

__version__ = "1.7"


class PGNewsBase(metaclass=ABCMeta):

    @abstractmethod
    def get_tasks(self):
        """ will be implemented in children"""

    @abstractmethod
    def process(self):
        """ will be implemented in children"""
