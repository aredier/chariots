import sure
import pytest

from chariots.core.ops import BaseOp, Merge
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.ops import Split
from chariots.core.markers import Number


class Foo(Number):
    def compatible(self, other):
        return isinstance(other, Foo)

class Bar(Number):
    def compatible(self, other):
        return isinstance(other, Bar)

class DevideTogether(BaseOp):
    markers = [Number()]
    requires = {"left": Foo(), "right": Bar()}
    name = "add_together"

    def _main(self, left, right):
        return left /  right

def test_simple_merge():
    single = DataTap(iter(range(1, 10)), Foo())
    double = DataTap(iter(range(2, 20, 2)), Bar())
    merged = Merge()([single, double])
    res = DevideTogether()(merged)
    for ind in res.perform():
        ind.should.be.a(dict)
        ind.should.have.key(DevideTogether.markers[0]).being.equal(0.5)


def test_simple_split(tap, add_op_cls, square_op_cls):
    split_1, split_2 = Split(2)(tap)
    add = add_op_cls()(split_1)
    square = square_op_cls()(split_2)
    for i, ind in enumerate(add.perform()):
        ind.should.be.a(dict)
        ind.should.have.key(add.markers[0]).being.equal(i + 1)
    for i, ind in enumerate(square.perform()):
        ind.should.be.a(dict)
        ind.should.have.key(square.markers[0]).being.equal(i ** 2)