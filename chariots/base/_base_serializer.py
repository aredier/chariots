"""serializer's abstract classes"""
from abc import ABC, abstractmethod
from typing import Any


class BaseSerializer(ABC):
    """
    serializers are helper classes for communication and persistence through out the `Chariots` framework.
    There mostly used by data nodes and and MLOps.

    For instance if you want to make a pipeline that downloads the iris dataset splits it between train and test and use
    two different formats for the train and test (please don't ...):

    .. testsetup::

        >>> from chariots import Pipeline
        >>> from chariots.nodes import Node, DataSavingNode
        >>> from chariots.serializers import CSVSerializer, DillSerializer
        >>> from chariots._helpers.doc_utils import IrisDF, TrainTestSplit

    .. doctest::

        >>> save_train_test = Pipeline([
        ...     Node(IrisDF(), output_nodes='df'),
        ...     Node(TrainTestSplit(), input_nodes=['df'], output_nodes=['train_df', 'test_df']),
        ...     DataSavingNode(serializer=CSVSerializer(), path='/train.csv', input_nodes=['train_df']),
        ...     DataSavingNode(serializer=DillSerializer(), path='/test.pkl', input_nodes=['test_df'])
        ... ], "save")

    fot MLOps if you want to change the default serialization format (for the model to be saved), you will need to
    change the `serializer_cls` class attribute
    """

    @abstractmethod
    def serialize_object(self, target: Any) -> bytes:
        """
        serializes the object into bytes (for ml ops `target` will be the model itself and not the op, for the data ops
        the `target` will be the input of the node )

        :param target: the object that will be serialized

        :return: the bytes of the serialized object
        """

    @abstractmethod
    def deserialize_object(self, serialized_object: bytes) -> Any:
        """
        returns the deserialized object from serialized bytes (that will be loaded from a saver)

        :param serialized_object: the serialized bytes

        :return: the deserialized objects
        """
