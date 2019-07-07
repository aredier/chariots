import pytest

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

            def __call__(self, input_list):
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

        return CounterModulo
    return gen


@pytest.fixture
def Range10():

    class Range(AbstractOp):

        def __call__(self, *args, **kwargs):
            return list(range(10))
    return Range


@pytest.fixture
def NotOp():

    class NotOp(AbstractOp):

        def __call__(self, input):
            return [not i for i in input]

    return NotOp


@pytest.fixture
def IsPair():

    class Inner(AbstractOp):

        def __call__(self, data):
            return [not i % 2 for i in data]
    return Inner
