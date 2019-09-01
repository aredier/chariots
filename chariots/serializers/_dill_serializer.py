from typing import Any

import dill

from chariots.base._base_serializer import BaseSerializer


class DillSerializer(BaseSerializer):
    """
    serializes the object into dill readable byte
    """

    def serialize_object(self, target: Any) -> bytes:
        return dill.dumps(target)

    def deserialize_object(self, serialized_object: bytes) -> Any:
        return dill.loads(serialized_object)