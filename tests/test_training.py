from sklearn.linear_model import LinearRegression
import sure
import numpy as np

from chariots.core.taps import DataTap
from chariots.core.markers import Marker
from chariots.core.markers import Matrix
from chariots.core.ops import Merge
from chariots.core.versioning import Signature
from chariots.training.trainable_op import TrainableOp


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
        self._model = LinearRegression()

    def _train_function(self, x, y):
        x = np.asarray(x).reshape((-1, 1))
        y = np.asarray(y)
        idx = np.array(list(range(x.shape[0])))
        np.random.shuffle(idx)
        x = x[idx, :]
        y = y[idx]
        self._model.fit(x , np.asarray(y))
    
    def _main(self, x):
        print(x)
        return self._model.predict(np.asarray(x).reshape(-1, 1))


def test_training_op():
    x = DataTap(iter(list(range(0, i + 10)) for i in range(2)), XMarker())
    y = DataTap(iter(list(range(1, i + 11)) for i in range(2)), YMarker())
    training_data =  Merge()([x, y])
    foo = LinearModel()
    foo.fit(training_data)
    x_test = DataTap(iter([i] for i in range(100)), XMarker())
    y_pred = foo(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(LinearModel.markers[0])
        float(y_pred_ind[LinearModel.markers[0]][0]).should.equal(i + 1., epsilon=0.0001)