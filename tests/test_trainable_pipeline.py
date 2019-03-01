import numpy as np
import pytest
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import SGDRegressor

from chariots.core.taps import DataTap
from chariots.core.markers import Marker, Number
from chariots.core.markers import Matrix
from chariots.core.ops import Merge, BaseOp, Split
from chariots.core.versioning import Signature
from chariots.training.trainable_op import TrainableOp
from chariots.training.trainable_pipeline import TrainablePipeline


class LeftMarker(Matrix):
    def compatible(self, other):
        return isinstance(other, LeftMarker)


class RightMarker(Matrix):
    def compatible(self, other):
        return isinstance(other, RightMarker)

@pytest.fixture
def left_linear_model_cls(linear_model_cls):
    class LinearModelL(linear_model_cls):
        markers = [LeftMarker(None,)]
    
    return LinearModelL


@pytest.fixture
def right_linear_model_cls(linear_model_cls):
    class LinearModelR(linear_model_cls):
        markers = [RightMarker((None,))]
    
    return LinearModelR


class Add(BaseOp):
    requires = {"left": LeftMarker(None,), "right": RightMarker(None,)}
    markers = [Matrix(None,)]
    signature = Signature(name = "add_together")

    def _main(self, left, right):
        return [l + r for l, r in zip(left, right)]


class Identity(BaseOp):
    requires = {"in_value" : Number()}
    signature = Signature(name = "id")
    markers = [Number()]

    def _main(self, in_value):
        return in_value


def test_trainable_pipeline_single_op(x_op_cls, linear_y_op_cls, linear_model_cls, x_marker_cls):
    numbers = np.random.choice(list(range(100)), 10, replace=True)

    data = DataTap(iter(numbers), Number())
    x = x_op_cls()(data)
    x, y = Split(2)(x)
    y = linear_y_op_cls()(y)
    training_data =  Merge()([x, y])
    model = linear_model_cls()
    pipe = TrainablePipeline()
    pipe.add(model)
    pipe.fit(training_data)
    x_test = DataTap(iter([i] for i in range(100)), x_marker_cls())
    y_pred = pipe(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(linear_model_cls.markers[0])
        float(y_pred_ind[linear_model_cls.markers[0]][0]).should.equal(i + 1., epsilon=0.01)


def test_trainable_pipeline_single_ignore_y(x_op_cls, linear_y_op_cls, linear_model_cls, x_marker_cls):
    numbers = np.random.choice(list(range(100)), 10, replace=True)
    data = DataTap(iter(numbers), Number())
    x_in = x_op_cls()(data)
    x, y = Split(2)(x_in)
    y = linear_y_op_cls()(y)
    training_data =  Merge()([x, y])
    model = linear_model_cls()(training_data)
    pipe = TrainablePipeline(x_in, model)
    pipe.fit(data)
    x_test = DataTap(iter([i] for i in range(100)), x_marker_cls())
    y_pred = pipe(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(LinearModel.markers[0])
        float(y_pred_ind[linear_model_cls.markers[0]][0]).should.equal(i + 1., epsilon=0.01)


def test_trainable_pipeline_parrallel(x_op_cls, linear_y_op_cls, left_linear_model_cls, 
                                      right_linear_model_cls, x_marker_cls):
    numbers = np.random.choice(list(range(100)), 10, replace=True)
    data = DataTap(iter(numbers), Number())
    data_id = Identity()(data)
    data_1, data_2 = Split(2)(data_id)

    # first pipeline
    x_in = x_op_cls()(data_1)
    x, y = Split(2)(x_in)
    y = linear_y_op_cls()(y)
    training_data =  Merge()([x, y])
    model = left_linear_model_cls()(training_data)

    # second pipeline
    x_in_2 = x_op_cls()(data_2)
    x_2, y_2 = Split(2)(x_in_2)
    y_2 = linear_y_op_cls()(y_2)
    training_data_2 =  Merge()([x_2, y_2])
    model_2 = right_linear_model_cls()(training_data_2)

    combined = Merge()([model_2, model])
    res = Add()(combined)

    pipe = TrainablePipeline(data_id, res)
    pipe.fit(data)
    x_test = DataTap(iter([i] for i in range(100)), x_marker_cls())
    y_pred = pipe(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(LinearModel.markers[0])
        float(y_pred_ind[LinearModel.markers[0]][0]).should.equal(i + 1., epsilon=0.01)