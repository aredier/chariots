# pylint: disable=no-member
"""
module that provides importable and picklable ops for tests that need them
"""
import subprocess
import time

import numpy as np
from tensorflow.keras import callbacks, models, layers, optimizers
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression

from ..pipelines import Pipeline, ops, nodes
from ..ml import MLMode, keras, sklearn
from .. import versioning


class IsPair(ops.BaseOp):
    """op that tests whether each elements of the input are pair or not"""

    def execute(self, data):  # pylint: disable=arguments-differ
        return [not i % 2 for i in data]


class WaitOp(ops.BaseOp):
    """op that sleeps for one second"""

    def execute(self, data):  # pylint: disable=arguments-differ
        time.sleep(.1)
        return data


class SKLROp(sklearn.SKSupervisedOp):
    """sci-kit learn linear regression op"""
    model_class = versioning.VersionedField(LinearRegression, versioning.VersionType.MINOR)


class YOp(ops.BaseOp):
    """op that returns a range from 1, 11 to be used as y in tests"""

    def execute(self):  # pylint: disable=arguments-differ
        return list(range(1, 11))


class PCAOp(sklearn.SKUnsupervisedOp):
    """sci-kit learn PCA op"""
    training_update_version = versioning.VersionType.MAJOR
    model_parameters = versioning.VersionedFieldDict(
        versioning.VersionType.MAJOR, {
            'n_components': 2,
        })
    model_class = versioning.VersionedField(PCA, versioning.VersionType.MAJOR)


class XTrainOpL(ops.BaseOp):
    """op that returns an array with three ranges as columns to be used as X in tests"""

    def execute(self):  # pylint: disable=arguments-differ
        return np.array([range(10), range(1, 11), range(2, 12)]).T


class KerasLogistic(keras.KerasOp):
    """logistic regression implemented in keras op"""

    input_params = versioning.VersionedFieldDict(versioning.VersionType.MINOR, {
        'epochs': 200,
        'batch_size': 100,
        'callbacks': [callbacks.EarlyStopping(monitor='mean_absolute_error')]
    })

    def _init_model(self):

        model = models.Sequential([
            layers.BatchNormalization(input_shape=(1,)),
            layers.Dense(1)
        ])
        model.compile(loss='mse', optimizer=optimizers.RMSprop(lr=0.1), metrics=['mae'])
        return model


class LinearDataSet(ops.BaseOp):
    """data set op with linear relationship between X and y"""

    def __init__(self, rows=10, op_callbacks=None):
        super().__init__(op_callbacks=op_callbacks)
        self.rows = rows

    def execute(self):  # pylint: disable=arguments-differ
        return (np.array([[i] for i in range(self.rows) for _ in range(10)]),
                np.array([i + 1 for i in range(self.rows) for _ in range(10)]))


class ToArray(ops.BaseOp):
    """op that transforms a list into array"""

    def __init__(self, output_shape=(-1, 1), op_callbacks=None):
        super().__init__(op_callbacks=op_callbacks)
        self.output_shape = output_shape

    def execute(self, input_data):  # pylint: disable=arguments-differ
        return np.array(input_data).reshape(self.output_shape)


class FromArray(ops.BaseOp):
    """input that transforms an array into list"""

    def execute(self, input_data):  # pylint: disable=arguments-differ
        return input_data.tolist()


class RQWorkerContext:
    """context helper that setups the rq workers needed for async pipeline executions"""

    def __init__(self):
        self.proc = None

    def __enter__(self):
        self.proc = subprocess.Popen('rq worker chariots_workers', shell=True)
        time.sleep(0.5)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.proc.kill()


def build_keras_pipeline(train_async=None, pred_async=None):
    """builds basic pipelines for testing the keras api in different setups"""
    train = Pipeline([
        nodes.Node(LinearDataSet(rows=10), output_nodes=['X', 'y']),
        nodes.Node(KerasLogistic(MLMode.FIT, verbose=0), input_nodes=['X', 'y'])
    ], 'train', use_worker=train_async)

    pred = Pipeline([
        nodes.Node(ToArray(output_shape=(-1, 1)), input_nodes=['__pipeline_input__'], output_nodes='X'),
        nodes.Node(KerasLogistic(MLMode.PREDICT, verbose=0), input_nodes=['X'], output_nodes='pred'),
        nodes.Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], 'pred', use_worker=pred_async)
    return train, pred


def do_keras_pipeline_predictions_test(pred_pipeline, client):
    """test the predictions produced by the above keras pipelines"""
    inputs = [[[5]]]
    pred = client.call_pipeline(pred_pipeline, inputs).value
    assert len(pred) == 1
    for batch_predictions, batch_inputs in zip(pred, inputs):
        assert len(batch_predictions) == 1
        assert batch_inputs[0][0] < batch_predictions[0] < batch_inputs[0][0] + 2
