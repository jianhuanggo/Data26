from abc import ABCMeta, abstractmethod


class PGDaemonBase(metaclass=ABCMeta):

    def __init__(self, args):
        self.args = args

    @abstractmethod
    def run(self):
        """ Implemented in child class"""


