import sure
import pytest

from chariots.core.versioning import Signature
from chariots.core.ops import BaseOp
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.markers import Number

class AddOneOp(BaseOp):
    markers = [Number()]
    requires = {"in_value": Number()}
    signature = Signature(name = "add")

    def _main(self, in_value):
        return in_value + 1

@pytest.fixture
def add_op():
    return AddOneOp()

class Square(BaseOp):
    markers = [Number()]
    requires = {"in_value": Number()}
    signature = Signature(name = "square")

    def _main(self, in_value):
        return in_value ** 2

@pytest.fixture
def square_op():
    return Square()

@pytest.fixture
def tap():
    return DataTap(iter(range(10)), Number())

def test_single_op(add_op, tap):
    add = add_op(tap)
    for i, res in enumerate(add.perform()):
        res.should.be.a(dict)
        res.should.have.key(add.markers[0]).being(i + 1)

def test_chained_op(tap, add_op, square_op):
    add = add_op(tap)
    square = square_op(add)
    for i, res in enumerate(square.perform()):
        res.should.be.a(dict)
        res.should.have.key(square.markers[0]).being((i + 1) ** 2)

def test_pipeline_add(tap, add_op, square_op):
    pipe = Pipeline()
    pipe.add(add_op)
    pipe.add(square_op)
    for i, res in enumerate(pipe(tap).perform()):
        res.should.be.a(dict)
        res.should.have.key(Square.markers[0]).being((i + 1) ** 2)


def test_pipeline_init(tap, add_op, square_op):
    add = add_op(tap)
    square = square_op(add)
    pipe = Pipeline(tap, square)
    for i, res in enumerate(pipe(tap).perform()):
        res.should.be.a(dict)
        res.should.have.key(square.markers[0]).being((i + 1) ** 2)

def test_pipeline_as_an_op(tap, add_op, square_op):
    add = add_op(tap)
    pipe = Pipeline()
    pipe.add(square_op)
    pipe(add)
    for i, res in enumerate(pipe.perform()):
        res.should.be.a(dict)
        res.should.have.key(square_op.markers[0]).being((i + 1) ** 2)
