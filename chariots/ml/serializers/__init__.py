"""
Serializers are utils classes that are used throughout the `Chariots` framework to transform objects into bytes.
there are for instance used to serialize the inner models of the machine learning ops:

.. testsetup

    >>> from typing import Any

    >>> from sklearn.decomposition import PCA

    >>> from chariots.ml.sklearn import SKSupervisedOp, SKSupervisedOp
    >>> from chariots.ml.serializers import BaseSerializer, BaseSerializer
    >>> class MySerializerCls(BaseSerializer):
    ...
    ...     def serialize_object(self, target: Any) -> bytes:
    ...         pass
    ...
    ...     def deserialize_object(self, serialized_object: bytes) -> Any:
    ...         pass

.. doctest::

    >>> class LinearRegression(SKSupervisedOp):
    ...
    ...     serializer_cls = MySerializerCls
    ...
    ...     model_class = PCA
"""
from ._base_serializer import BaseSerializer
from ._csv_serialzer import CSVSerializer
from ._dill_serializer import DillSerializer
from ._json_serializer import JSONSerializer

__all__ = [
    'BaseSerializer',
    'DillSerializer',
    'JSONSerializer',
    'CSVSerializer',
]
