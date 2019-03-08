from chariots.core.ops import Split
from chariots.core.ops import Merge
from chariots.core.ops import BaseOp
from chariots.training.evaluation import ClassificationMetrics
from chariots.training.evaluation import RegresionMetrics
from chariots.core.markers import Number
from chariots.core.markers import Matrix


class YMarker(Matrix):

    def compatible(self, other):
        return isinstance(other, YMarker)


class XMarker(Matrix):

    def compatible(self, other):
        return isinstance(other, XMarker)


class TrueY(BaseOp):
    requires = {"input_number": Number()}
    markers = [YMarker((1, 1))]

    def _main(self, input_number):
        return [1]


class FalseY(BaseOp):
    requires = {"input_number": Number()}
    markers = [YMarker((1, 1))]

    def _main(self, input_number):
        return [0]


class XOp(BaseOp):
    requires = {"input_number": Number()}
    markers = [XMarker((1, 1))]

    def _main(self, input_number):
        return [1]


def test_classification_metric_correct(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = TrueY()(y_pred)
    evaluation = ClassificationMetrics(XMarker((1, 1)), YMarker((1, 1)), ["accuracy"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("accuracy").being.equal(1)
    

def test_classification_metric_false(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = FalseY()(y_pred)
    evaluation = ClassificationMetrics(XMarker((1, 1)), YMarker((1, 1)), ["accuracy"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("accuracy").being.equal(0)


def test_regression_metric_correct(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = TrueY()(y_pred)
    evaluation = RegresionMetrics(XMarker((1, 1)), YMarker((1, 1)), ["mae", "mse"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("mae").being.equal(0)
    

def test_regresion_metric_false(tap, add_op_cls):
    y_true, y_pred = Split(2)(tap)
    y_true = XOp()(y_true)
    y_pred = FalseY()(y_pred)
    evaluation = RegresionMetrics(XMarker((1, 1)), YMarker((1, 1)), ["mae", "mse"])
    res = Merge()([y_true, y_pred]) 
    evaluation = evaluation.evaluate(res)
    evaluation.should.be.a(dict)
    evaluation.should.have.key(str(res.version)).being.a(dict)
    evaluation[str(res.version)].should.have.key("mae").being.equal(1)