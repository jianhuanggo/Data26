from abc import ABCMeta, abstractmethod

__version__ = "1.5"


class PGReleaseBase(metaclass=ABCMeta):

    @abstractmethod
    def release_note(self):
        """ Implement in the Child Class"""
