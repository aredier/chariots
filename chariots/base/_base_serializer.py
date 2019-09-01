from abc import ABC, abstractmethod
from typing import Any


class BaseSerializer(ABC):
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