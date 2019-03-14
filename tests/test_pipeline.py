import sure
import pytest

from chariots.core.ops import BaseOp
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.requirements import Number


def test_single_op(add_op_cls, tap):
    add = add_op_cls()(tap)
    for i, res in enumerate(add.perform()):
        res.should.be.a(dict)
        res.should.have.key(add_op_cls.markers[0]).being(i + 1)

def test_chained_op(tap, add_op_cls, square_op_cls):
    add = add_op_cls()(tap)
    square = square_op_cls()(add)
    for i, res in enumerate(square.perform()):
        res.should.be.a(dict)
        res.should.have.key(square_op_cls.markers[0]).being((i + 1) ** 2)

def test_pipeline_add(tap, add_op_cls, square_op_cls):
    pipe = Pipeline()
    pipe.add(add_op_cls())
    pipe.add(square_op_cls())
    for i, res in enumerate(pipe(tap).perform()):
        res.should.be.a(dict)
        res.should.have.key(square_op_cls.markers[0]).being((i + 1) ** 2)


def test_pipeline_init(tap, add_op_cls, square_op_cls):

    add = add_op_cls()(tap)
    square = square_op_cls()(add)
    pipe = Pipeline(tap, square)
    for i, res in enumerate(pipe.perform()):
        res.should.be.a(dict)
        res.should.have.key(square_op_cls.markers[0]).being((i + 1) ** 2)

def test_pipeline_as_an_op(tap, add_op_cls, square_op_cls):
    add = add_op_cls()(tap)
    pipe = Pipeline()
    pipe.add(square_op_cls())
    pipe(add)
    for i, res in enumerate(pipe.perform()):
        res.should.be.a(dict)
        res.should.have.key(square_op_cls.markers[0]).being((i + 1) ** 2)
