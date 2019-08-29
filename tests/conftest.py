import pytest

import numpy as np

from chariots.core.ops import LoadableOp, AbstractOp
from chariots.core.saving import DillSerializer
from chariots.core.versioning import VersionedField, VersionType


@pytest.fixture
def savable_op_generator():

    def gen(counter_step=1):

        class CounterModulo(LoadableOp):

            step = VersionedField(counter_step, VersionType.MAJOR)

            def __init__(self):
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

    class Range(AbstractOp):

        def execute(self, *args, **kwargs):
            return list(range(10))
    return Range


@pytest.fixture
def AddOne():

    class AddOneInner(AbstractOp):

        def execute(self, input_value):
            return [i + 1 for i in input_value]

    return AddOneInner


@pytest.fixture
def NotOp():

    class NotOp(AbstractOp):

        def execute(self, input):
            return [not i for i in input]

    return NotOp


@pytest.fixture
def IsPair():

    class IsPairInner(AbstractOp):

        def execute(self, data):
            return [not i % 2 for i in data]
    return IsPairInner


@pytest.fixture
def XTrainOp():
    class XTrainOpInner(AbstractOp):

        def execute(self):
            return np.array(range(10)).reshape(-1, 1)
    return XTrainOpInner


@pytest.fixture
def YOp():
    class YOpInner(AbstractOp):

        def execute(self):
            return list(range(1, 11))
    return YOpInner
