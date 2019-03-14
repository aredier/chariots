import pytest
import sure
import numpy as np
from sklearn.linear_model import SGDRegressor

from chariots.core.ops import BaseOp
from chariots.core.markers import Number
from chariots.core.markers import Requirement
from chariots.core.markers import Matrix
from chariots.core.taps import DataTap
from chariots.training.trainable_op import TrainableOp


class AddOneOp(BaseOp):
    markers = [Number]
    requires = {"input_value": Number}
    name = "add"

    def _main(self, input_value):
        return input_value + 1


@pytest.fixture
def add_op_cls():
    return AddOneOp


class Square(BaseOp):
    markers = [Number]
    requires = {"input_value": Number}
    name = "square"

    def _main(self, input_value):
        return input_value ** 2

@pytest.fixture
def square_op_cls():
    return Square


@pytest.fixture
def tap():
    return DataTap(iter(range(10)), Number)


XRequirement = Requirement.new_marker()
YRequirement = Requirement.new_marker()


@pytest.fixture
def x_requirement_cls():
    return XRequirement


@pytest.fixture
def y_requirement_cls():
    return YRequirement


class LinearModel(TrainableOp):
    training_requirements = {"x": XRequirement, "y": YRequirement}
    requires ={"x": XRequirement} 
    name = "linear_model"
    markers = [Matrix.with_shape_and_dtype((None, 1), np.float32)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._model = SGDRegressor(eta0=8e-5, learning_rate="constant", verbose=1)

    def _inner_train(self, x, y):
        idx = np.random.choice(list(range(len(x))), len(x), replace=False)
        x = np.asarray(x).reshape((-1, 1))[idx, :]
        y = np.asarray(y)[idx]
        self._model = self._model.partial_fit(x , np.asarray(y))
    
    def _main(self, x):
        return self._model.predict(np.asarray(x).reshape(-1, 1))

@pytest.fixture
def linear_model_cls():
    return LinearModel

class XOp(BaseOp):
    requires = {"in_value" : Number}
    name = "x"
    markers = [XRequirement]

    def _main(self, in_value):
        return np.random.choice(list(range(in_value, in_value + 100)), 80000)


@pytest.fixture
def x_op_cls():
    return XOp

class LinearYOp(BaseOp):
    requires = {"in_value" : XRequirement}
    name = "y"
    markers = [YRequirement]

    def _main(self, in_value):
        return np.array([in_value + 1 for in_value in in_value])

@pytest.fixture
def linear_y_op_cls():
    return LinearYOp