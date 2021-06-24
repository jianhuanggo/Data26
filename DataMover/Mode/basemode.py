from abc import ABCMeta, abstractmethod


class BaseMode(metaclass=ABCMeta):

    @abstractmethod
    def get_mode(self):
        pass

    @abstractmethod
    def source_apply(self, query):
        pass

    @abstractmethod
    def target_apply(self, query):
        pass


