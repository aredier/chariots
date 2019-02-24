import sure
import pytest

from chariots.core.versioning import Signature
from chariots.core.base_op import BaseOp, Merge
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.base_op import Split
from chariots.core.markers import Number

class AddOneOp(BaseOp):
    markers = [Number()]
    requires = {"input_value": Number()}
    signature = Signature(name = "add")

    def _main(self, input_value):
        return input_value + 1


class Square(BaseOp):
    markers = [Number()]
    requires = {"input_value": Number()}
    signature = Signature(name = "square")

    def _main(self, input_value):
        return input_value ** 2

class Foo(Number):
    def compatible(self, other):
        return isinstance(other, Foo)

class Bar(Number):
    def compatible(self, other):
        return isinstance(other, Bar)

class AddTogether(BaseOp):
    markers = [Number()]
    requires = {"left": Foo(), "right": Bar()}
    signature = Signature(name="add_together")

    def _main(self, left, right):
        return left +  right

@pytest.fixture
def tap():
    return DataTap(iter(range(10)), Number())

def test_simple_merge():
    pos = DataTap(iter(range(10)), Foo())
    neg = DataTap(iter(range(0, -10, -1)), Bar())
    merged = Merge()([pos, neg])
    res = AddTogether()(merged)
    for ind in res.perform():
        ind.should.be.a(dict)
        ind.should.have.key(AddTogether.markers[0]).being.equal(0)


def test_simple_split(tap):
    split_1, split_2 = Split(2)(tap)
    add = AddOneOp()(split_1)
    square = Square()(split_2)
    for i, ind in enumerate(add.perform()):
        ind.should.be.a(dict)
        ind.should.have.key(add.markers[0]).being.equal(i + 1)
    for i, ind in enumerate(square.perform()):
        ind.should.be.a(dict)
        ind.should.have.key(square.markers[0]).being.equal(i ** 2)