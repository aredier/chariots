import numpy as np
import pytest
from keras import layers
from keras import models

from chariots.core import ops
from chariots.core import taps
from chariots.core import requirements
from chariots.core import saving
from chariots.training import keras


XMarker = keras.KerasInput.create_child()
YMarker = keras.KerasOutput.create_child()


class XTrain(ops.BaseOp):
    def _main(self, x_in: requirements.Number) -> XMarker:
        """takes a number and returns an array of numbers between 0 and this One"""
        return np.random.rand(10).reshape(-1, 1) * 10 


class YTrain(ops.BaseOp):
    def _main(self, x: XMarker) -> YMarker:
        return x + 1


class XTest(ops.BaseOp):
    def _main(self, x_in: requirements.Number) -> XMarker:
        return np.array([[x_in]])


@pytest.fixture
def model():
    model = models.Sequential([
        layers.Dense(64, input_shape=(1,)),
        layers.Dense(1),
    ])
    model.compile(loss="mse", optimizer="adam")
    return model


def test_keras_model_cls_def(model):
    class KerasLinear(keras.KerasOp):
        def _build_model(self):
            return model
    x_train = taps.DataTap(iter(range(1000)), requirements.Number)
    x_train = XTrain()(x_train)
    x_train, y_train = ops.Split(2)(x_train)
    y_train = YTrain()(y_train)
    model = KerasLinear()
    model.fit(ops.Merge()([x_train, y_train]))

    x_test = taps.DataTap(iter(range(10)), requirements.Number)
    x_test = XTest()(x_test)
    for i, y_pred in enumerate(model(x_test).perform()):
        float(y_pred[keras.KerasOutput][0][0]).should.equal(i + 1., epsilon = 0.1)


def test_keras_model_factory(model):
    KerasLinear = keras.KerasOp.factory(build_function=lambda: model, name="KerasLinear")
    x_train = taps.DataTap(iter(range(1000)), requirements.Number)
    x_train = XTrain()(x_train)
    x_train, y_train = ops.Split(2)(x_train)
    y_train = YTrain()(y_train)
    model = KerasLinear()
    model.fit(ops.Merge()([x_train, y_train]))

    x_test = taps.DataTap(iter(range(10)), requirements.Number)
    x_test = XTest()(x_test)
    for i, y_pred in enumerate(model(x_test).perform()):
        float(y_pred[keras.KerasOutput][0][0]).should.equal(i + 1., epsilon = 0.1)

        
def test_keras_model_saving(model):
    KerasLinear = keras.KerasOp.factory(build_function=lambda: model, name="KerasLinear")
    x_train = taps.DataTap(iter(range(1000)), requirements.Number)
    x_train = XTrain()(x_train)
    x_train, y_train = ops.Split(2)(x_train)
    y_train = YTrain()(y_train)
    model_op = KerasLinear()
    model_op.fit(ops.Merge()([x_train, y_train]))
    saver = saving.FileSaver()
    model_op.save(saver)

    del(model_op)
    model_op = KerasLinear.load(saver)
    x_test = taps.DataTap(iter(range(10)), requirements.Number)
    x_test = XTest()(x_test)
    for i, y_pred in enumerate(model_op(x_test).perform()):
        float(y_pred[keras.KerasOutput][0][0]).should.equal(i + 1., epsilon = 0.1)