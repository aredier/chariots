import json
import os
from abc import ABC, abstractmethod
from typing import Any, Text

import dill


class Serializer(ABC):

    ObjectType = Any

    @abstractmethod
    def serialize_object(self, target: ObjectType) -> bytes:
        pass

    @abstractmethod
    def deserialize_object(self, serialized_object: bytes) -> ObjectType:
        pass


class DillSerializer(Serializer):

    ObjectType = Any

    def serialize_object(self, target: ObjectType) -> bytes:
        return dill.dumps(target)

    def deserialize_object(self, serialized_object: bytes) -> ObjectType:
        return dill.loads(serialized_object)


class JSONSerializer(Serializer):
    ObjectType = Any

    def serialize_object(self, target: ObjectType) -> bytes:
        return json.dumps(target).encode("utf-8")

    def deserialize_object(self, serialized_object: bytes) -> ObjectType:
        object_json = serialized_object.decode("utf-8")
        return json.loads(object_json)


class Saver(ABC):

    def save(self, serialized_object: bytes, path: Text) -> bool:
        pass

    def load(self, path: Text) -> bytes:
        pass


class FileSaver(Saver):

    def __init__(self, root_path: os.PathLike):
        self.root_path = root_path

    def build_path(self, path: Text):
        if path[0] == "/":
            path = path[1:]
        return os.path.join(self.root_path, path)

    def save(self, serialized_object: bytes, path: Text) -> bool:
        object_path = self.build_path(path)
        dirname = os.path.dirname(object_path)
        print(dirname, object_path)
        os.makedirs(dirname, exist_ok=True)
        with open(object_path, "wb") as file:
            file.write(serialized_object)
        return True

    def load(self, path: Text) -> bytes:
        object_path = self.build_path(path)
        with open(object_path, "rb") as file:
            return file.read()
