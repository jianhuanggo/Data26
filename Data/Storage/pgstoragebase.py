from abc import ABCMeta, abstractmethod
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator

__version__ = "1.7"


class PGStorageBase(metaclass=ABCMeta):

    @abstractmethod
    def inst(self):
        """ Implement in the Child Class """

    @abstractmethod
    def get_storage_name_and_path(self, dirpath: str) -> Tuple[Union[str, None], Union[str, None]]:
        """ Implement in the Child Class """

    @abstractmethod
    def save(self, data: object, storage_format: str = None, storage_parameter: dict = None, *args, **kwargs):
        """ Implement in the Child Class """

    @abstractmethod
    def load(self, location: str, storage_format: str = None, storage_parameter: dict = None, *args, **kwargs) -> bool:
        """ Implement in the Child Class """


