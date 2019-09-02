from abc import ABC
from typing import Text


class BaseSaver(ABC):
    """
    abstraction of a file system used to persist/load assets and ops this can be used on the actual local file system
    of the machine the `Chariots` server is running or on a bottomless storage service (not implemented, PR welcome)

    To create a new Saver class you only need to define the `Save` and `Load` behaviors

    :param root_path: the root path to use when mounting the saver (for instance the base path to use in the
                      the file system when using the `FileSaver`)
    """

    def __init__(self, root_path: Text):
        self.root_path = root_path

    def save(self, serialized_object: bytes, path: Text) -> bool:
        """
        saves bytes to a specific path.

        :param serialized_object: the bytes to persist
        :param path: the path to save the bytes to. You should not include the `root_path` of the saver in this path:
                     saving to `/foo/bar.txt` on a saver with `/my/root/path` as root path will create/update
                     `/my/root/path/foo/bar.txt`

        :return: whether or not the object was correctly serialized.
        """
        pass

    def load(self, path: Text) -> bytes:
        """
        loads the bytes serialized at a specific path

        :param path: the path to load the bytes from.You should not include the `root_path` of the saver in this path:
                     loading to `/foo/bar.txt` on a saver with `/my/root/path` as root path will load
                     `/my/root/path/foo/bar.txt`

        :return: saved bytes
        """
        pass
