import pytest
import sure

from chariots.core.ops import BaseOp, Merge
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.ops import Split
from chariots.core.requirements import Number
from chariots.core.requirements import Matrix
from chariots.core.requirements import FloatType

class GenerateArray(BaseOp):
    markers = [Matrix.with_shape_and_dtype((5,), FloatType)]
    requires = {"input_value": Number}
    name = "array_gen"

    def _main(self, input_value):
        return [input_value for _ in range(5)]

class Sum(BaseOp):
    markers = [Number]
    requires = {"array": GenerateArray}
    name = "sum_array"
    
    def _main(self, array):
        return sum(array)


@pytest.fixture
def tap():
    return DataTap(iter(range(10)), Number)

def test_wrong_requires(tap, add_op_cls, square_op_cls):
    add = add_op_cls()(tap)
    array = GenerateArray()(add)
    square = square_op_cls()
    square.when.called_with(array).should.throw(ValueError)


def test_op_as_requirement(tap, add_op_cls):
    array = GenerateArray()(tap)
    sum_op = Sum()
    sum_op.when.called_with(array)

    sum_op = Sum()
    sum_op.when.called_with(tap).should.throw(ValueError)

def test_marker_generation(x_requirement_cls, y_requirement_cls):
    new_marker_cls = x_requirement_cls.create_child()
    assert x_requirement_cls.compatible(new_marker_cls)
    assert not new_marker_cls.compatible(y_requirement_cls)
    assert new_marker_cls.compatible(new_marker_cls)

def test_op_markers(add_op_cls, square_op_cls):
    assert add_op_cls.markers[0].compatible(add_op_cls.as_marker())
    assert not square_op_cls.as_marker().compatible(add_op_cls.as_marker())
    assert not add_op_cls.as_marker().compatible(square_op_cls.as_marker())
    assert add_op_cls.as_marker().compatible(add_op_cls.as_marker())