import numpy as np 

from chariots.core.ops import Split
from chariots.core.ops import Merge
from chariots.core.ops import BaseOp
from chariots.training.evaluation import ClassificationMetrics
from chariots.training.evaluation import RegresionMetrics
from chariots.core.requirements import Number
from chariots.core.requirements import Matrix
from chariots.core.requirements import FloatType
from chariots.core.taps import DataTap

YMarker = Matrix.create_child()
YPred = Matrix.create_child() 
XMarker = Matrix.create_child()

class TrueY(BaseOp):
    requires = {"input_number": Number}
    markers = [YMarker.with_shape_and_dtype((1, 1), FloatType)]

    def _main(self, input_number):
        return [1]


class FalseY(BaseOp):
    requires = {"input_number": Number}
    markers = [YMarker.with_shape_and_dtype((1, 1), FloatType)]

    def _main(self, input_number):
        return [0]


class XOp(BaseOp):
    requires = {"input_number": Number}
    markers = [XMarker.with_shape_and_dtype((1, 1), FloatType)]

    def _main(self, input_number):
        return [1]


def test_classification_metric_correct(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = TrueY()(y_pred)
    evaluation = ClassificationMetrics(XMarker.with_shape_and_dtype((1, 1), FloatType), 
                                       YMarker.with_shape_and_dtype((1, 1), FloatType),
                                       ["accuracy"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("accuracy").being.equal(1)
    

def test_classification_metric_false(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = FalseY()(y_pred)
    evaluation = ClassificationMetrics(XMarker.with_shape_and_dtype((1, 1), FloatType), 
                                       YMarker.with_shape_and_dtype((1, 1), FloatType),
                                       ["accuracy"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("accuracy").being.equal(0.)


def test_regression_metric_correct(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = TrueY()(y_pred)
    evaluation = RegresionMetrics(XMarker.with_shape_and_dtype((1, 1), FloatType), 
                                  YMarker.with_shape_and_dtype((1, 1), FloatType),
                                  ["mae", "mse"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("mae").being.equal(0)
    evaluation[str(res.version)].should.have.key("mse").being.equal(0)
    

def test_regresion_metric_false(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = FalseY()(y_pred)
    evaluation = RegresionMetrics(XMarker.with_shape_and_dtype((1, 1), FloatType), 
                                  YMarker.with_shape_and_dtype((1, 1), FloatType),
                                  ["mae", "mse"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("mae").being.equal(1.)
    evaluation[str(res.version)].should.have.key("mse").being.equal(1.)

def test_classification_metric_correct(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = TrueY()(y_pred)
    evaluation = ClassificationMetrics(XMarker.with_shape_and_dtype((1, 1), FloatType), 
                                       YMarker.with_shape_and_dtype((1, 1), FloatType),
                                       ["accuracy"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("accuracy").being.equal(1)

def test_attaching_evaluaton(x_op_cls, linear_y_op_cls, linear_model_cls, x_requirement_cls):
    numbers = np.random.choice(list(range(100)), 10, replace=True)

    data = DataTap(iter(numbers), Number)
    x = x_op_cls()(data)
    x, y = Split(2)(x)
    y = linear_y_op_cls()(y)
    training_data =  Merge()([x, y])
    foo = linear_model_cls()
    foo.fit(training_data)
    YTruReq = Matrix.create_child()
    evaluation = RegresionMetrics(y_true=YTruReq, y_pred=foo.markers[0])
    foo.attach_evaluation(evaluation)
    x_test = DataTap(iter([i] for i in range(100)), x_requirement_cls)
    y_true = DataTap(iter([i+1] for i in range(100)), YTruReq)
    test_data = Merge()([x_test, y_true])
    evaluation = foo.evaluate(test_data)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(foo.version)).being.a(dict)
    assert evaluation[str(foo.version)]["mae"] < 0.1
    assert evaluation[str(foo.version)]["mse"] < 0.1

