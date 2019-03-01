import pytest

from chariots.core.ops import BaseOp
from chariots.core.versioning import Signature
from chariots.core.markers import Number
from chariots.core.markers import Matrix
from chariots.core.taps import DataTap


class AddOneOp(BaseOp):
    markers = [Number()]
    requires = {"input_value": Number()}
    signature = Signature(name = "add")

    def _main(self, input_value):
        return input_value + 1


@pytest.fixture
def add_op_cls():
    return AddOneOp


class Square(BaseOp):
    markers = [Number()]
    requires = {"input_value": Number()}
    signature = Signature(name = "square")

    def _main(self, input_value):
        return input_value ** 2

@pytest.fixture
def square_op_cls():
    return Square


@pytest.fixture
def tap():
    return DataTap(iter(range(10)), Number())