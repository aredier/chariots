"""module to allow serializing in CSV format"""
import io

# to avoid circular imports
from ..base._base_serializer import BaseSerializer


try:
    import pandas as pd
except ImportError:
    pass


class CSVSerializer(BaseSerializer):
    """
    A serializer to save a pandas data frame.

    :raises Typeerror: if the node receives something other than a pandas `DataFrame`
    """

    def serialize_object(self, target: pd.DataFrame) -> bytes:
        if not isinstance(target, pd.DataFrame):
            raise TypeError('can only serialize pandas data frames to csv')
        return target.to_csv(index=None).encode('utf-8')

    def deserialize_object(self, serialized_object: bytes) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(serialized_object), encoding='utf8')
