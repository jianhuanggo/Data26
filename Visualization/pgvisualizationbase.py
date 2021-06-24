from abc import ABCMeta, abstractmethod, abstractstaticmethod


class PGVisualizationBase(metaclass=ABCMeta):

    @abstractmethod
    def visual(self, *args, **kwargs):
        """ Implement in the Child Class"""

    @abstractstaticmethod
    def inst():
        """ Implement in the Child Class"""
