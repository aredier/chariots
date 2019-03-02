import pytest
import sure

from chariots.core.versioning import Signature
from chariots.core.ops import BaseOp, Merge
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.ops import Split
from chariots.core.markers import Number, Matrix

class GenerateArray(BaseOp):
    markers = [Matrix((5,))]
    requires = {"input_value": Number()}
    signature = Signature(name="array_gen")

    def _main(self, input_value):
        return [input_value for _ in range(5)]

@pytest.fixture
def tap():
    return DataTap(iter(range(10)), Number())

def test_wrong_requires(tap, add_op_cls, square_op_cls):
    add = add_op_cls()(tap)
    array = GenerateArray()(add)
    square = square_op_cls()
    square.when.called_with(array).should.throw(ValueError)

def test_marker_generation(x_marker_cls, y_marker_cls):
    new_marker_cls = x_marker_cls.new_marker()
    assert x_marker_cls().compatible(new_marker_cls())
    assert not new_marker_cls().compatible(x_marker_cls())
    assert new_marker_cls().compatible(new_marker_cls())