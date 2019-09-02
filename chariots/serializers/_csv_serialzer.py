import io

from chariots.errors import BackendError
from .._helpers.optional_libraries import load_pandas
# to avoid circular imports
from ..base._base_serializer import BaseSerializer

try:
    pd = load_pandas()

    class CSVSerializer(BaseSerializer):
        """
        A serializer to save a pandas data frame.

        :raises Typeerror: if the node receives something other than a pandas `DataFrame`
        """

        def serialize_object(self, target: pd.DataFrame) -> bytes:
            if not isinstance(target, pd.DataFrame):
                raise TypeError('can only serialize pandas data frames to csv')
            return target.to_csv().encode("utf-8")

        def deserialize_object(self, serialized_object: bytes) -> pd.DataFrame:
            return pd.read_csv(io.BytesIO(serialized_object), encoding="utf8")
except BackendError:
    pass
