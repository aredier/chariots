import io

from chariots.errors import BackendError
from .._helpers.optional_libraries import load_pandas
# to avoid circular imports
from ..base._base_serializer import BaseSerializer

try:
    pd = load_pandas()

    class CSVSerializer(BaseSerializer):
        """
        serializes a pandas data frame to and from csv format
        """

        def serialize_object(self, target: pd.DataFrame) -> bytes:
            return target.to_csv().encode("utf)8")

        def deserialize_object(self, serialized_object: bytes) -> pd.DataFrame:
            return pd.read_csv(io.BytesIO(serialized_object), encoding="utf8")
except BackendError:
    pass
