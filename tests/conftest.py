"""all the fixtures that might be used in both unit and integration tests"""
from typing import List, Optional

import pytest
import numpy as np

from chariots import MLMode, Pipeline
from chariots._helpers.test_helpers import XTrainOpL, PCAOp, SKLROp, YOp, FromArray
from chariots.base import BaseOp
from chariots.callbacks import OpCallBack
from chariots.nodes import Node
from chariots.ops import LoadableOp
from chariots.serializers import DillSerializer
from chariots.versioning import VersionType, VersionedField


@pytest.fixture
def savable_op_generator():
    """fixture that returns a function to generate a savable stateful op"""

    def gen(counter_step=1):
        """fixture generator func"""

        class CounterModulo(LoadableOp):
            """inner fixture op"""

            step = VersionedField(counter_step, VersionType.MAJOR)

            def __init__(self, op_callbacks: Optional[List[OpCallBack]] = None):
                super().__init__(op_callbacks=op_callbacks)
                self.count = 0

            def execute(self, input_list):  # pylint: disable=arguments-differ
                self.count += self.step
                return [not i % self.count for i in input_list]

            def load(self, serialized_object: bytes):
                serializer = DillSerializer()
                print('loading ', serializer.deserialize_object(serialized_object))
                self.count = serializer.deserialize_object(serialized_object)

            def serialize(self) -> bytes:
                serializer = DillSerializer()
                print('saving', self.count)
                return serializer.serialize_object(self.count)

        print('counter modulo version', CounterModulo.__version__)
        return CounterModulo
    return gen


@pytest.fixture
def Range10():  # pylint: disable=invalid-name
    """fixture op that takes no input and returns a range(10) as output"""

    class Range(BaseOp):
        """inner fixture op"""

        def execute(self, *args, **kwargs):
            return list(range(10))
    return Range


@pytest.fixture
def AddOne():  # pylint: disable=invalid-name
    """fixture op that adds one to each element in the input list"""

    class AddOneInner(BaseOp):
        """inner fixture op"""

        def execute(self, input_value):  # pylint: disable=arguments-differ
            return [i + 1 for i in input_value]

    return AddOneInner


@pytest.fixture
def NotOp():  # pylint: disable=invalid-name
    """
    fixture op that returns a list where each element is the not opperation applied to the corresponding element in the
     input
    """

    class NotOpInner(BaseOp):
        """inner fixture op"""

        def execute(self, op_input):  # pylint: disable=arguments-differ
            return [not i for i in op_input]

    return NotOpInner


@pytest.fixture
def IsPair():  # pylint: disable=invalid-name
    """
    fixture op that takes list of int in input and returns a list of bool in retrurn (each bool is whether the
    corresponding int is pair or not)
    """

    class IsPairInner(BaseOp):
        """inner fixture op"""

        def execute(self, data):  # pylint: disable=arguments-differ
            return [not i % 2 for i in data]
    return IsPairInner


@pytest.fixture
def XTrainOp():  # pylint: disable=invalid-name
    """fixture for a `range(10)` training data"""
    class XTrainOpInner(BaseOp):
        """inner fixture op"""

        def execute(self):  # pylint: disable=arguments-differ
            return np.array(range(10)).reshape(-1, 1)
    return XTrainOpInner


@pytest.fixture()
def complex_sk_pipelines():
    """
    complex sci-kit learn based pipelines with:
    * a pca training pipeline
    * a model training pipeline
    * a prediction pipeline
    """
    train_transform = Pipeline([
        Node(XTrainOpL(), output_nodes='x_raw'),
        Node(PCAOp(mode=MLMode.FIT), input_nodes=['x_raw'], output_nodes='x_train'),
    ], 'train_pca')
    train_pipe = Pipeline([
        Node(XTrainOpL(), output_nodes='x_raw'),
        Node(YOp(), output_nodes='y_train'),
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=['x_raw'], output_nodes='x_train'),
        Node(SKLROp(mode=MLMode.FIT), input_nodes=['x_train', 'y_train'])
    ], 'train')
    pred_pipe = Pipeline([
        Node(PCAOp(mode=MLMode.PREDICT), input_nodes=['__pipeline_input__'], output_nodes='x_train'),
        Node(SKLROp(mode=MLMode.PREDICT), input_nodes=['x_train'], output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], 'pred')
    return train_transform, train_pipe, pred_pipe


@pytest.fixture
def basic_sk_pipelines(XTrainOp):  # pylint: disable=invalid-name, redefined-outer-name
    """builds a basic sci-kit learn training and prediction pipeline"""

    train_pipe = Pipeline([
        Node(XTrainOp(), output_nodes='x_train'),
        Node(YOp(), output_nodes='y_train'),
        Node(SKLROp(mode=MLMode.FIT), input_nodes=['x_train', 'y_train'])
    ], name='train')
    pred_pipe = Pipeline([
        Node(SKLROp(mode=MLMode.PREDICT), input_nodes=['__pipeline_input__'], output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], name='pred')
    return train_pipe, pred_pipe
