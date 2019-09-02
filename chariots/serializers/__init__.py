"""
Serializers are utils classes that are used throughout the `Chariots` framework to transform objects into bytes.
there are for instance used to serialize the inner models of the machine learning ops:

.. testsetup

    >>> from sklearn.decomposition import PCA

    >>> from chariots.sklearn import SKSupervisedOp
    >>> from chariots.base import BaseSerializer
    >>> from chariots.sklearn import SKSupervisedOp
    >>> from chariots.base import BaseSerializer
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

there are also usually used in the saving nodes to choose the serialization method for your datasets:

.. testsetup::

    >>> from chariots.nodes import DataSavingNode
    >>> from chariots.serializers import CSVSerializer

.. doctest::

    >>> saving_node = DataSavingNode(serializer=CSVSerializer(), path='my_path.csv', input_nodes=["my_dataset"])
"""
from typing import Any

from ._csv_serialzer import CSVSerializer
from ._dill_serializer import DillSerializer
from ._json_serializer import JSONSerializer

__all__ = [
    "DillSerializer",
    "JSONSerializer",
    "CSVSerializer",
]
