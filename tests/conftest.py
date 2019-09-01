from typing import List, Optional

import pytest
import numpy as np

from chariots.base._base_op import BaseOp
from chariots.callbacks._op_callback import OpCallBack
from chariots.ops._loadable_op import LoadableOp
from chariots.serializers._dill_serializer import DillSerializer
from chariots.versioning._version_type import VersionType
from chariots.versioning._versioned_field import VersionedField


@pytest.fixture
def savable_op_generator():

    def gen(counter_step=1):

        class CounterModulo(LoadableOp):

            step = VersionedField(counter_step, VersionType.MAJOR)

            def __init__(self, callbacks: Optional[List[OpCallBack]] = None):
                super().__init__(callbacks=callbacks)
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

        def execute(self, input):
            return [not i for i in input]

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
