import os

import pytest
import pandas as pd
import numpy as np 

from chariots.io import csv
from chariots.core import ops
from chariots.core.markers import Matrix, Number

class UnwrapOp(ops.BaseOp):
    markers = [Number()]
    requires = {"input_data": Matrix((None, 1))}

    def _main(self, input_data):
        return sum(input_data.flatten())



@pytest.fixture
def csv_file(tmp_path):
    data = pd.DataFrame([
        [i]
        for i in range(10)
    ], columns=["x"])
    path = os.path.join(tmp_path, "foo.csv")
    data.to_csv(path)
    return path
    

def test_csv_tap_no_batch_single_col(csv_file):
    with csv.CSVTap(csv_file, {Matrix((None, 1)): ["x"]}) as tap:
        res = UnwrapOp()(tap)
        res_as_a_list = list(res.perform())
        res_as_a_list.should.have.length_of(1)
        res_as_a_list[0].should.be.a(dict)
        res_as_a_list[0].should.have.key(UnwrapOp.markers[0])
        int(res_as_a_list[0][UnwrapOp.markers[0]]).should.be.equal(sum(range(10)))