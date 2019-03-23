import copy

import numpy as np
import pytest
from sklearn.linear_model import LinearRegression, SGDRegressor

from chariots.core.ops import BaseOp, Merge, Split
from chariots.core.pipeline import Pipeline
from chariots.core.requirements import Matrix, Number, Requirement
from chariots.core.taps import DataTap
from chariots.training.trainable_op import TrainableOp

LeftMarker = Matrix.create_child()


RightMarker = Matrix.create_child()

@pytest.fixture
def left_linear_model_cls(linear_model_cls):
    class LinearModelL(linear_model_cls):
        name = "left-model"
        markers = [LeftMarker]

    return LinearModelL


@pytest.fixture
def right_linear_model_cls(linear_model_cls):
    class LinearModelR(linear_model_cls):
        name = "right-model"
        markers = [RightMarker]

    return LinearModelR


class Add(BaseOp):
    requires = {"left": LeftMarker, "right": RightMarker}
    markers = [Matrix]
    name = "add_together"

    def _main(self, left, right):
        return [l + r for l, r in zip(left, right)]


class Identity(BaseOp):
    name = "id"

    def _main(self, in_value: Number) -> Number:
        return in_value


def test_trainable_pipeline_single_op(x_op_cls, linear_y_op_cls, linear_model_cls,
                                      x_requirement_cls, forget_version_op_cls):
    numbers = np.random.choice(list(range(100)), 20, replace=True)

    data = DataTap(iter(numbers), Number)
    x = x_op_cls()(data)
    x, y = Split(2)(x)
    y = linear_y_op_cls()(y)
    x = forget_version_op_cls()(x)
    training_data =  Merge()([x, y])
    model = linear_model_cls()
    pipe = Pipeline()
    pipe.add(model)
    pipe.fit(training_data)
    x_test = DataTap(iter([i] for i in range(100)), x_requirement_cls)
    x_test = forget_version_op_cls()(x_test)
    y_pred = pipe(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(linear_model_cls.markers[0])
        float(y_pred_ind[linear_model_cls.markers[0]][0]).should.equal(i + 1., epsilon=0.01)
    x = Split(2)

@pytest.fixture
def model_pipeline(linear_model_cls, forget_version_op_cls, x_requirement_cls):
    forgret_op = forget_version_op_cls(x_requirement_cls)
    model = linear_model_cls()(forgret_op)
    return  Pipeline(forgret_op, model)

def test_training_pipeline(x_op_cls, linear_y_op_cls, x_requirement_cls, model_pipeline, linear_model_cls):
    numbers = np.random.choice(list(range(100)), 10, replace=True)
    data = DataTap(iter(numbers), Number)
    x_in = x_op_cls()
    x, y = Split(2)(x_in)
    y = linear_y_op_cls()(y)
    training_data =  Merge()([x, y])
    model_pipeline(training_data)
    training_pipeline = Pipeline(x_in, model_pipeline)
    training_pipeline.fit(data)

    x_test = DataTap(iter([i] for i in range(100)), x_requirement_cls)
    y_pred = model_pipeline(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(linear_model_cls.markers[0])
        float(y_pred_ind[linear_model_cls.markers[0]][0]).should.equal(i + 1., epsilon=0.01)


def test_saving_trained_pipeline(x_op_cls, linear_y_op_cls, x_requirement_cls, linear_model_cls,
                           model_pipeline, saver):
    virgin_pipeline = copy.deepcopy(model_pipeline)
    numbers = np.random.choice(list(range(100)), 10, replace=True)
    data = DataTap(iter(numbers), Number)
    x_in = x_op_cls()
    x, y = Split(2)(x_in)
    y = linear_y_op_cls()(y)
    training_data =  Merge()([x, y])
    model_pipeline(training_data)
    training_pipeline = Pipeline(x_in, model_pipeline)
    training_pipeline.fit(data)
    model_pipeline.save(saver)

    x_test = DataTap(iter([i] for i in range(100)), x_requirement_cls)
    assert not virgin_pipeline.ready
    virgin_pipeline.load(saver)
    y_pred = virgin_pipeline(x_test)
    print(virgin_pipeline.output_op.fited)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(linear_model_cls.markers[0])
        float(y_pred_ind[linear_model_cls.markers[0]][0]).should.equal(i + 1., epsilon=0.01)


def test_trainable_pipeline_parrallel(x_op_cls, linear_y_op_cls, left_linear_model_cls,
                                      right_linear_model_cls, x_requirement_cls,
                                      forget_version_op_cls):
    numbers = np.random.choice(list(range(100)), 20, replace=True)

    data = DataTap(iter(numbers), Number)
    data_id = Identity()(data)
    data_1, data_2 = Split(2)(data_id)

    # first pipeline
    x_in = x_op_cls()(data_1)
    x, y = Split(2)(x_in)
    x = forget_version_op_cls()(x)
    y = linear_y_op_cls()(y)
    training_data =  Merge()([x, y])
    model = left_linear_model_cls()(training_data)

    # second pipeline
    x_in_2 = x_op_cls()(data_2)
    x_2, y_2 = Split(2)(x_in_2)
    x_2 = forget_version_op_cls()(x_2)
    y_2 = linear_y_op_cls()(y_2)
    training_data_2 =  Merge()([x_2, y_2])
    model_2 = right_linear_model_cls()(training_data_2)

    combined = Merge()([model_2, model])
    res_op = Add()
    res = res_op(combined)

    pipe = Pipeline(data_id, res)
    pipe.fit(data)
    x_test = DataTap(iter(range(100)), x_requirement_cls)
    x_test = forget_version_op_cls()(x_test)
    x_test_l, x_test_r = Split(2)(x_test)
    y_l = model(x_test_l)
    y_r = model_2(x_test_r)
    y_pred = res_op(Merge()([y_l, y_r]))
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(Matrix)
        float(y_pred_ind[Matrix][0]).should.equal(2. * (i +1), epsilon=0.01)
