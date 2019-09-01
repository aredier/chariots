from abc import ABC
from typing import Text


class BaseSaver(ABC):
    """
    abstraction of a file system used to persist/load assets and ops
    """

    def save(self, serialized_object: bytes, path: Text) -> bool:
        """
        saves bytes to a path

        :param serialized_object: the bytes to persist
        :param path: the path to save the bytes to
        :return: whether or not the object was correctly serialized
        """
        pass

    def load(self, path: Text) -> bytes:
        """
        loads persisted bytes from a specific path

        :param path: the path to load the bytes from
        :return: said bytes
        """
        pass


