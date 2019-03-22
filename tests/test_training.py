import numpy as np

from chariots.core.taps import DataTap
from chariots.core.requirements import Number
from chariots.core.ops import Merge, Split


def test_training_op(x_op_cls, linear_y_op_cls, linear_model_cls, x_requirement_cls, 
                     forget_version_op_cls):
    numbers = np.random.choice(list(range(100)), 10, replace=True)

    data = DataTap(iter(numbers), Number)
    x = x_op_cls()(data)
    x, y = Split(2)(x)
    y = linear_y_op_cls()(y)
    training_data =  Merge()([x, y])
    training_data = forget_version_op_cls()(training_data)
    foo = linear_model_cls()
    foo.fit(training_data)
    x_test = DataTap(iter([i] for i in range(100)), x_requirement_cls)
    x_test = forget_version_op_cls()(x_test)
    y_pred = foo(x_test)
    for i, y_pred_ind in enumerate(y_pred.perform()):
        y_pred_ind.should.be.a(dict)
        y_pred_ind.should.have.key(linear_model_cls.markers[0])
        float(y_pred_ind[linear_model_cls.markers[0]][0]).should.equal(i + 1., epsilon=0.01)