from abc import ABCMeta, abstractmethod

__version__ = "1.7"


class PGRealestateBase(metaclass=ABCMeta):

    @abstractmethod
    def get_tasks(self, pg_dataset, parameters: dict = None):
        """ will be implemented in children"""

    @abstractmethod
    def process(self, pg_dataset, *args: object, **kwargs: object):
        """ will be implemented in children"""
