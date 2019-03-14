import os

import pytest
import pandas as pd
import numpy as np 

from chariots.io import csv
from chariots.core import ops
from chariots.core.markers import Matrix, Number, FloatType

XMarker = Matrix.new_marker()
YMarker = Matrix.new_marker()
XYMarker = Matrix.new_marker()
ZMarker = Matrix.new_marker()

class UnwrapOp(ops.BaseOp):
    markers = [Number]
    requires = {"input_data": Matrix.with_shape_and_dtype((None, 1), FloatType)}

    def _main(self, input_data):
        return sum(input_data.flatten())

class SumOp(ops.BaseOp):
    markers = [Number]
    requires= {"left": XMarker.with_shape_and_dtype((None, 1), FloatType),
               "right": YMarker.with_shape_and_dtype((None, 1), FloatType)}

    def _main(self, left, right):
        return sum(l + r for l, r in zip(left, right))

class GroupedSumOp(ops.BaseOp):
    markers = [Number]
    requires= {"xy": XYMarker.with_shape_and_dtype((None, 1), FloatType),
               "z": ZMarker.with_shape_and_dtype((None, 1), FloatType)}

    def _main(self, xy, z):
        xy_sum = sum(x + y for x, y in xy)
        return xy_sum + sum(z.flatten())


@pytest.fixture
def csv_file(tmp_path):
    data = pd.DataFrame([
        [i, 2 * i, -i]
        for i in range(10)
    ], columns=["x", "y", "z"])
    path = os.path.join(tmp_path, "foo.csv")
    data.to_csv(path)
    return path


def test_csv_tap_no_batch_single_col(csv_file):
    with csv.CSVTap(csv_file, {Matrix.with_shape_and_dtype((None, 1), FloatType): ["x"]}) as tap:
        res = UnwrapOp()(tap)
        res_as_a_list = list(res.perform())
        res_as_a_list.should.have.length_of(1)
        res_as_a_list[0].should.be.a(dict)
        res_as_a_list[0].should.have.key(UnwrapOp.markers[0])
        int(res_as_a_list[0][UnwrapOp.markers[0]]).should.be.equal(sum(range(10)))


def test_csv_tap_no_batch_multiple_cols(csv_file):
    with csv.CSVTap(csv_file, {XMarker.with_shape_and_dtype((None, 1), FloatType): ["x"],
                               YMarker.with_shape_and_dtype((None, 1), FloatType): ["y"]}) as tap:
        res = SumOp()(tap)
        res_as_a_list = list(res.perform())
        res_as_a_list.should.have.length_of(1)
        res_as_a_list[0].should.be.a(dict)
        res_as_a_list[0].should.have.key(SumOp.markers[0])
        int(res_as_a_list[0][SumOp.markers[0]]).should.be.equal(3 * sum(range(10)))

def test_csv_tap_no_batch_grouped_cols(csv_file):
    with csv.CSVTap(csv_file, {XYMarker.with_shape_and_dtype((None, 1), FloatType): ["x", "y"],
                               ZMarker.with_shape_and_dtype((None, 1), FloatType): ["z"]}) as tap:
        res = GroupedSumOp()(tap)
        res_as_a_list = list(res.perform())
        res_as_a_list.should.have.length_of(1)
        res_as_a_list[0].should.be.a(dict)
        res_as_a_list[0].should.have.key(GroupedSumOp.markers[0])
        int(res_as_a_list[0][GroupedSumOp.markers[0]]).should.be.equal(2 * sum(range(10)))

def test_csv_tap_batched_single_col(csv_file):
    with csv.CSVTap(csv_file, {Matrix.with_shape_and_dtype((None, 1), FloatType): ["x"]},
                               batch_size=1, batches=10) as tap:
        res = UnwrapOp()(tap)
        for i, res_ind in enumerate(res.perform()):
            res_ind.should.be.a(dict)
            res_ind.should.have.key(UnwrapOp.markers[0])
            int(res_ind[UnwrapOp.markers[0]]).should.be.equal(i)


def test_csv_tap_batched_multiple_cols(csv_file):
    with csv.CSVTap(csv_file, {XMarker.with_shape_and_dtype((None, 1), FloatType): ["x"],
                               YMarker.with_shape_and_dtype((None, 1), FloatType): ["y"]},
                               batch_size=1, batches=10) as tap:
        res = SumOp()(tap)
        for i, res_ind in enumerate(res.perform()):
            res_ind.should.be.a(dict)
            res_ind.should.have.key(SumOp.markers[0])
            int(res_ind[SumOp.markers[0]]).should.be.equal(3 * i)

def test_csv_tap_batched_grouped_cols(csv_file):
    with csv.CSVTap(csv_file, {XYMarker.with_shape_and_dtype((None, 1), FloatType): ["x", "y"],
                               ZMarker.with_shape_and_dtype((None, 1), FloatType): ["z"]},
                               batch_size=1, batches=10) as tap:
        res = GroupedSumOp()(tap)
        for i, res_ind in enumerate(res.perform()):
            res_ind.should.be.a(dict)
            res_ind.should.have.key(GroupedSumOp.markers[0])
            int(res_ind[GroupedSumOp.markers[0]]).should.be.equal(2 * i)