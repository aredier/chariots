from typing import List, Optional

import pytest
import numpy as np

from chariots.base import BaseOp
from chariots.callbacks import OpCallBack
from chariots.ops import LoadableOp
from chariots.serializers import DillSerializer
from chariots.versioning import VersionType, VersionedField


@pytest.fixture
def savable_op_generator():

    def gen(counter_step=1):

        class CounterModulo(LoadableOp):

            step = VersionedField(counter_step, VersionType.MAJOR)

            def __init__(self, op_callbacks: Optional[List[OpCallBack]] = None):
                super().__init__(op_callbacks=op_callbacks)
                self.count = 0

            def execute(self, input_list):
                self.count += self.step
                return [not i % self.count for i in input_list]

            def load(self, serialized_object: bytes):
                serializer = DillSerializer()
                print("loading ", serializer.deserialize_object(serialized_object))
                self.count = serializer.deserialize_object(serialized_object)

            def serialize(self) -> bytes:
                serializer = DillSerializer()
                print("saving", self.count)
                return serializer.serialize_object(self.count)

        print("counter modulo version", CounterModulo.__version__)
        return CounterModulo
    return gen


@pytest.fixture
def Range10():

    class Range(BaseOp):

        def execute(self, *args, **kwargs):
            return list(range(10))
    return Range


@pytest.fixture
def AddOne():

    class AddOneInner(BaseOp):

        def execute(self, input_value):
            return [i + 1 for i in input_value]

    return AddOneInner


@pytest.fixture
def NotOp():

    class NotOp(BaseOp):

        def execute(self, op_input):
            return [not i for i in op_input]

    return NotOp


@pytest.fixture
def IsPair():

    class IsPairInner(BaseOp):

        def execute(self, data):
            return [not i % 2 for i in data]
    return IsPairInner


@pytest.fixture
def XTrainOp():
    class XTrainOpInner(BaseOp):

        def execute(self):
            return np.array(range(10)).reshape(-1, 1)
    return XTrainOpInner


@pytest.fixture
def YOp():
    class YOpInner(BaseOp):

        def execute(self):
            return list(range(1, 11))
    return YOpInner

@pytest.fixture
def LinearDataSet():

    class LinearDataSetOp(BaseOp):

        def __init__(self, rows=10, op_callbacks=None):
            super().__init__(op_callbacks=op_callbacks)
            self.rows = rows

        def execute(self):
            return (np.array([[i] for i in range(self.rows) for _ in range(10)]),
                    np.array([i + 1 for i in range(self.rows) for _ in range(10)]))

    return LinearDataSetOp

@pytest.fixture
def ToArray():

    class ToArrayOp(BaseOp):

        def __init__(self, output_shape=(-1, 1), op_callbacks=None):
            super().__init__(op_callbacks=op_callbacks)
            self.output_shape = output_shape

        def execute(self, input_data):
            return np.array(input_data).reshape(self.output_shape)

    return ToArrayOp


@pytest.fixture
def FromArray():

    class FromArrayOp(BaseOp):

        def execute(self, input_data):
            return input_data.tolist()

    return FromArrayOp
