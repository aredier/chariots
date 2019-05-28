from typing import Optional, Mapping, Text, Any

import sure
import pytest

from chariots.core.ops import BaseOp
from chariots.core.ops import Merge
from chariots.core.dataset import DataSet
from chariots.core.taps import DataTap
from chariots.core.pipeline import Pipeline
from chariots.core.requirements import Number
from chariots.core.requirements import Number


LeftNumber = Number.create_child()
RightNumber = Number.create_child()


def create_callback():
    outer_results = []

    def monitor_in_and_out(op_res, op_input):
        outer_results.append({
            "in": op_input,
            "out": op_res,
        })
    return monitor_in_and_out, outer_results


class FirstOp(BaseOp):
    def _main(self, data: LeftNumber) -> LeftNumber:
        return data


class SecondOp(BaseOp):
    def _main(self, data: RightNumber) -> RightNumber:
        return data


class Sum(BaseOp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stored_results = []

    def _main(self, left: LeftNumber, right: RightNumber) -> Number:
        return left + right

    def __callback__(self, op_res: Optional[Mapping[Text, Any]], op_input: Optional[Mapping[Text, Any]]):
        self.stored_results.append({
            "in": op_input,
            "out": op_res,
        })


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


def test_jumping_ops():
    left_tap = DataTap(iter(range(10)), LeftNumber)
    right_tap = DataTap(iter(range(10)), RightNumber)
    full_tap = Merge()([left_tap, right_tap])
    left = FirstOp()(full_tap)
    right = SecondOp()(left)
    res = Sum()(right)
    for i, batch in enumerate(res.perform()):
        batch.should.have.key(Number).being.equal(2 * i)
                


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

def test_callback_native():
    left_tap = DataTap(iter(range(10)), LeftNumber)
    right_tap = DataTap(iter(range(10)), RightNumber)
    full_tap = Merge()([left_tap, right_tap])
    left = FirstOp()(full_tap)
    right = SecondOp()(left)
    sum_op = Sum()
    res = sum_op(right)
    for i, batch in enumerate(res.perform()):
        batch.should.have.key(Number).being.equal(2 * i)
    assert len(sum_op.stored_results) == 10
    print(sum_op.requires, LeftNumber)
    for i, ind_res in enumerate(sum_op.stored_results):
        assert ind_res == {
            "in": {
                "left": i,
                "right": i
            },
            "out": {
                Number: 2 * i
            }
        }


def test_callback_added(add_op_cls, tap):
    op = add_op_cls()
    monitor_in_and_out, outer = create_callback()
    op.register_callback(monitor_in_and_out)
    add = op(tap)
    for i, res in enumerate(add.perform()):
        res.should.be.a(dict)
        res.should.have.key(add_op_cls.markers[0]).being(i + 1)

    assert len(outer) == 10
    for i, ind_res in enumerate(outer):
        assert ind_res == {
            "in": {
                "input_value": i,
            },
            "out": {
                Number: i + 1
            }
        }

