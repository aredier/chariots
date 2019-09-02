import json
from typing import Any

# to avoid circular imports
from ..base._base_serializer import BaseSerializer


class JSONSerializer(BaseSerializer):
    """
    serializes objects into JSON format
    """

    def serialize_object(self, target: Any) -> bytes:
        return json.dumps(target).encode("utf-8")

    def deserialize_object(self, serialized_object: bytes) -> Any:
        object_json = serialized_object.decode("utf-8")
        return json.loads(object_json)
