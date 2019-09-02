from typing import Any

import dill

# to avoid circular imports
from ..base._base_serializer import BaseSerializer


class DillSerializer(BaseSerializer):
    """
    serializes objects using the dill library (similar to pickle but optimized for numpy arrays.
    """

    def serialize_object(self, target: Any) -> bytes:
        return dill.dumps(target)

    def deserialize_object(self, serialized_object: bytes) -> Any:
        return dill.loads(serialized_object)
