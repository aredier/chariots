from sklearn.linear_model import LinearRegression
from sklearn.linear_model import SGDRegressor
import sure
import numpy as np

from chariots.core.taps import DataTap
from chariots.core.markers import Marker, Number
from chariots.core.markers import Matrix
from chariots.core.ops import Merge, BaseOp, Split
from chariots.core.versioning import Signature
from chariots.training.trainable_op import TrainableOp
from chariots.training.trainable_pipeline import TrainablePipeline


class XMarker(Marker):

    def compatible(self, other):
        return isinstance(other, XMarker)

class YMarker(Marker):

    def compatible(self, other):
        return isinstance(other, YMarker)


class LinearModel(TrainableOp):
    training_requirements = {"x": XMarker(), "y": YMarker()}
    requires ={"x": XMarker()} 
    signature = Signature(name = "add")
    markers = [Matrix((1, 2))]

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

class XDAta(BaseOp):
    requires = {"in_value" : Number()}
    signature = Signature(name = "x")
    markers = [XMarker()]

    def _main(self, in_value):
        return np.random.choice(list(range(in_value, in_value + 100)), 80000)

class YDAta(BaseOp):
    requires = {"in_value" : XMarker()}
    signature = Signature(name = "y")
    markers = [YMarker()]

    def _main(self, in_value):
        return np.array([in_value + 1 for in_value in in_value])


def test_training_op():
    numbers = np.random.choice(list(range(100)), 10, replace=True)

    data = DataTap(iter(numbers), Number())
    x = XDAta()(data)
    x, y = Split(2)(x)
    y = YDAta()(y)
    training_data =  Merge()([x, y])
    foo = LinearModel()
    foo.fit(training_data)
    x_test = DataTap(iter([i] for i in range(100)), XMarker())
    y_pred = foo(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(LinearModel.markers[0])
        float(y_pred_ind[LinearModel.markers[0]][0]).should.equal(i + 1., epsilon=0.01)


def test_trainable_pipeline_single_op():
    numbers = np.random.choice(list(range(100)), 10, replace=True)

    data = DataTap(iter(numbers), Number())
    x = XDAta()(data)
    x, y = Split(2)(x)
    y = YDAta()(y)
    training_data =  Merge()([x, y])
    model = LinearModel()
    pipe = TrainablePipeline()
    pipe.add(model)
    pipe.fit(training_data)
    x_test = DataTap(iter([i] for i in range(100)), XMarker())
    y_pred = pipe(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(LinearModel.markers[0])
        float(y_pred_ind[LinearModel.markers[0]][0]).should.equal(i + 1., epsilon=0.01)