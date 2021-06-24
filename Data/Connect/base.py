from abc import ABCMeta, abstractmethod


class Base(metaclass=ABCMeta):

    @abstractmethod
    def get_table_list(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def execute(self, query):
        pass

    @property
    @abstractmethod
    def session(self):
        pass




