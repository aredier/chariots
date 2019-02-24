import pytest
import sure

from chariots.core.versioning import Signature
from chariots.core.base_op import BaseOp, Merge
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.base_op import Split
from chariots.core.markers import Number, Matrix

class AddOneOp(BaseOp):
    markers = [Number()]
    requires = {"input": Number()}
    signature = Signature(name = "add")

    def _main(self, tap):
        return tap + 1

class GenerateArray(BaseOp):
    markers = [Matrix((5,))]
    requires = {"input": Number()}
    signature = Signature(name="array_gen")

    def _main(self, add):
        return [add for _ in range(5)]

class Square(BaseOp):
    markers = [Number()]
    requires = {"input": Number()}
    signature = Signature(name = "square")

    def _main(self, tap):
        return tap ** 2

@pytest.fixture
def tap():
    return DataTap(iter(range(10)), Number())

def test_wrong_requires(tap):
    add = AddOneOp()(tap)
    array = GenerateArray()(add)
    square = Square()
    square.when.called_with(array).should.throw(ValueError)