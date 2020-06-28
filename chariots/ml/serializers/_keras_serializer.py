import os
import tempfile
from typing import Any

import tensorflow

from chariots.ml.serializers import BaseSerializer


class KerasSerializer(BaseSerializer):

    def serialize_object(self, target: Any) -> bytes:
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'model.h5')
            target.save(path)
            with open(path, 'rb') as bytes_file:
                return bytes_file.read()

    def deserialize_object(self, serialized_object: bytes) -> Any:
        with tempfile.TemporaryDirectory() as dir_path:
            path = os.path.join(dir_path, 'model.h5')
            with open(path, 'wb') as bytes_file:
                bytes_file.write(serialized_object)
            return tensorflow.keras.models.load_model(path)
