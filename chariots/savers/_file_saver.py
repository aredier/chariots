import os
from typing import Text

from chariots.base import BaseSaver


class FileSaver(BaseSaver):
    """
    a saver that persists to the local file system
    """

    def __init__(self, root_path: str):
        self.root_path = root_path

    def _build_path(self, path: Text):
        """
        builds the path on the file system from the saving path

        :param path: the path inside the saver (/ops/foo.pkl)
        :return: the path on the file system (/tmp/chariots/ops/foo.pkl)
        """
        if path[0] == "/":
            path = path[1:]
        return os.path.join(self.root_path, path)

    def save(self, serialized_object: bytes, path: Text) -> bool:
        object_path = self._build_path(path)
        dirname = os.path.dirname(object_path)
        os.makedirs(dirname, exist_ok=True)
        with open(object_path, "wb") as file:
            file.write(serialized_object)
        return True

    def load(self, path: Text) -> bytes:
        object_path = self._build_path(path)
        with open(object_path, "rb") as file:
            return file.read()