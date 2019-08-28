import io
import json
import os
from abc import ABC, abstractmethod
from typing import Any, Text

import dill

from chariots.helpers.optional_libraries import load_pandas, BackendError


class Serializer(ABC):
    """
    a Serializer handles transforming an object to and from bytes.
    """

    @abstractmethod
    def serialize_object(self, target: Any) -> bytes:
        """
        transforms an object into bytes

        :param target: the object to transform
        :return: the bytes of the serialized object
        """
        pass

    @abstractmethod
    def deserialize_object(self, serialized_object: bytes) -> Any:
        """
        loads the serialized bytes and returns the object they represent

        :param serialized_object: the serialized bytes
        :return: the deserialized objects
        """
        pass


class DillSerializer(Serializer):
    """
    serializes the object into dill readable byte
    """

    def serialize_object(self, target: Any) -> bytes:
        return dill.dumps(target)

    def deserialize_object(self, serialized_object: bytes) -> Any:
        return dill.loads(serialized_object)


class JSONSerializer(Serializer):
    """
    serializes the object into JSON format
    """

    def serialize_object(self, target: Any) -> bytes:
        return json.dumps(target).encode("utf-8")

    def deserialize_object(self, serialized_object: bytes) -> Any:
        object_json = serialized_object.decode("utf-8")
        return json.loads(object_json)


try:
    pd = load_pandas()

    class CSVSerializer(Serializer):
        """
        serializes a pandas data frame to and from csv format
        """

        def serialize_object(self, target: pd.DataFrame) -> bytes:
            return target.to_csv().encode("utf)8")

        def deserialize_object(self, serialized_object: bytes) -> pd.DataFrame:
            return pd.read_csv(io.BytesIO(serialized_object), encoding="utf8")
except BackendError:
    pass


class Saver(ABC):
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


class FileSaver(Saver):
    """
    a saver that persists to the local file system
    """

    def __init__(self, root_path: os.PathLike):
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
