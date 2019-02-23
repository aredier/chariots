import sure
import pytest

from chariots.core.versioning import Signature
from chariots.core.base_op import BaseOp, Merge
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.base_op import Split

class AddOneOp(BaseOp):
    signature = Signature(name = "add")

    def _main(self, tap):
        return tap + 1


class Square(BaseOp):
    signature = Signature(name = "square")

    def _main(self, tap):
        return tap ** 2

class AddTogether(BaseOp):
    signature = Signature(name="add_together")

    def _main(self, tap, idenitity):
        return tap +  idenitity

class Identity(BaseOp):
    signature = Signature(name = "idenitity")

    def _main(self, tap):
        return tap

@pytest.fixture
def tap():
    return DataTap(iter(range(10)))

def test_simple_merge():
    pos = DataTap(iter(range(10)))
    neg = DataTap(iter(range(0, -10, -1)))
    neg = Identity()(neg)
    merged = Merge()([pos, neg])
    res = AddTogether()(merged)
    for ind in res.perform():
        ind.should.be.a(dict)
        ind.should.have.key("add_together").being.equal(0)


def test_simple_split(tap):
    split_1, split_2 = Split(2)(tap)
    add = AddOneOp()(split_1)
    square = Square()(split_2)
    for i, ind in enumerate(add.perform()):
        ind.should.be.a(dict)
        ind.should.have.key("add").being.equal(i + 1)
    for i, ind in enumerate(square.perform()):
        ind.should.be.a(dict)
        ind.should.have.key("square").being.equal(i ** 2)