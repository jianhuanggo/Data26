from abc import ABCMeta, abstractmethod

__version__ = "1.5"


class PGProcessingBase(metaclass=ABCMeta):

    @abstractmethod
    def run(self, *args, **kwargs):
        """ Implemented in Child Class """

    #@abstractmethod
    #def set_param(self, *args, **kwargs):
    #    """ Implemented in Child Class """


