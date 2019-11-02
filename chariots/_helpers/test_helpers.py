"""
module that provides importable and picklable ops for tests that need them
"""
import subprocess
import time

import numpy as np
from keras import callbacks, models, layers, optimizers
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression

from chariots.base import BaseOp
from chariots.keras import KerasOp
from chariots.sklearn import SKSupervisedOp, SKUnsupervisedOp
from chariots.versioning import VersionedField, VersionType, VersionedFieldDict


class IsPair(BaseOp):

    def execute(self, data):
        return [not i % 2 for i in data]


class WaitOp(BaseOp):

    def execute(self, data):
        time.sleep(.1)
        return data


class SKLROp(SKSupervisedOp):
    model_class = VersionedField(LinearRegression, VersionType.MINOR)


class YOp(BaseOp):

    def execute(self):
        return list(range(1, 11))


class PCAOp(SKUnsupervisedOp):
    training_update_version = VersionType.MAJOR
    model_parameters = VersionedFieldDict(
        VersionType.MAJOR, {
            'n_components': 2,
        })
    model_class = VersionedField(PCA, VersionType.MAJOR)


class XTrainOpL(BaseOp):

    def execute(self):
        return np.array([range(10), range(1, 11), range(2, 12)]).T


class KerasLogistic(KerasOp):

    input_params = VersionedFieldDict(VersionType.MINOR, {
        'epochs': 200,
        'batch_size': 100,
        'callbacks': [callbacks.EarlyStopping(monitor='mean_absolute_error'),]
    })

    def _init_model(self):

        model = models.Sequential([
            layers.BatchNormalization(input_shape=(1,)),
            layers.Dense(1)
        ])
        model.compile(loss='mse', optimizer=optimizers.RMSprop(lr=0.1), metrics=['mae'])
        return model


class LinearDataSet(BaseOp):

    def __init__(self, rows=10, op_callbacks=None):
        super().__init__(op_callbacks=op_callbacks)
        self.rows = rows

    def execute(self):
        return (np.array([[i] for i in range(self.rows) for _ in range(10)]),
                np.array([i + 1 for i in range(self.rows) for _ in range(10)]))


class ToArray(BaseOp):

    def __init__(self, output_shape=(-1, 1), op_callbacks=None):
        super().__init__(op_callbacks=op_callbacks)
        self.output_shape = output_shape

    def execute(self, input_data):
        return np.array(input_data).reshape(self.output_shape)


class FromArray(BaseOp):

    def execute(self, input_data):
        return input_data.tolist()


class RQWorkerContext:

    def __init__(self):
        self.proc = None

    def __enter__(self):
        self.proc = subprocess.Popen('rq worker chariots_workers', shell=True)
        time.sleep(0.5)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.proc.kill()
