import pytest
import numpy as np
from flaky import flaky
from keras import models, layers, optimizers, callbacks

from chariots import Pipeline, MLMode, Chariots, TestClient
from chariots.base import BaseOp
from chariots.keras import KerasOp
from chariots.nodes import Node
from chariots.versioning import VersionedFieldDict, VersionType


@pytest.fixture
def KerasLogistic():

    class KerasLogisticOp(KerasOp):

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

    return KerasLogisticOp


@flaky(5, 1)
def test_train_keras_pipeline(KerasLogistic, LinearDataSet, tmpdir, ToArray, FromArray):

    train = Pipeline([
        Node(LinearDataSet(rows=10), output_nodes=['X', 'y']),
        Node(KerasLogistic(MLMode.FIT), input_nodes=['X', 'y'])
    ], 'train')

    pred = Pipeline([
        Node(ToArray(output_shape=(-1, 1)), input_nodes=['__pipeline_input__'], output_nodes='X'),
        Node(KerasLogistic(MLMode.PREDICT), input_nodes=['X'], output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], 'pred')
    my_app = Chariots(app_pipelines=[train, pred],
                      path=str(tmpdir), import_name="my_app")
    client = TestClient(my_app)
    client.call_pipeline(train)
    client.save_pipeline(train)
    client.load_pipeline(pred)

    pred = client.call_pipeline(pred, [[5]])
    assert len(pred) == 1
    assert len(pred[0]) == 1
    assert 5 < pred[0][0] < 7


@pytest.fixture
def MultiDataSet():

    class MultiDataSetop(BaseOp):

        def execute(self, ):
            return (
                [
                    np.array([[i] for i in range(10) for _ in range(10)]),
                    np.array([[-i] for i in range(10) for _ in range(10)])
                ],
               np.array([i + 1 for i in range(10) for _ in range(10)])
            )

    return MultiDataSetop


@pytest.fixture()
def MultiInputKeras():

    class MultiInputKerasOp(KerasOp):
        input_params = VersionedFieldDict(VersionType.MINOR, {
            'epochs': 200,
            'batch_size': 100,
            'callbacks': [callbacks.EarlyStopping(monitor='mean_absolute_error'), ]
        })

        def _init_model(self):
            input_a = layers.Input(shape=(1,))
            tower_a = layers.Dense(1)(input_a)

            input_b = layers.Input(shape=(1,))
            tower_b = layers.Dense(1)(input_b)

            output = layers.Concatenate()([tower_a, tower_b])
            output = layers.Dense(1)(output)

            model = models.Model([input_a, input_b], output)
            model.compile(loss='mse', optimizer=optimizers.RMSprop(lr=0.1), metrics=['mae'])
            return model

    return MultiInputKerasOp


@pytest.fixture
def CreateInputs():

    class CreateInputs(BaseOp):

        def execute(self, input_data):
            print('foo ')
            return [np.array(input_data[0]), np.array(input_data[1])]

    return CreateInputs


@flaky(5, 1)
def test_keras_multiple_datasets(MultiDataSet, FromArray, tmpdir, MultiInputKeras, CreateInputs):
    train = Pipeline([
        Node(MultiDataSet(), output_nodes=['X', 'y']),
        Node(MultiInputKeras(MLMode.FIT), input_nodes=['X', 'y'])
    ], 'train')

    pred = Pipeline([
        Node(CreateInputs(), input_nodes=['__pipeline_input__'], output_nodes='X'),
        Node(MultiInputKeras(MLMode.PREDICT), input_nodes=['X'], output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], 'pred')
    my_app = Chariots(app_pipelines=[train, pred, ],
                      path=str(tmpdir), import_name="my_app")
    client = TestClient(my_app)
    client.call_pipeline(train)
    client.save_pipeline(train)
    client.load_pipeline(pred)

    pred = client.call_pipeline(pred, [[[5]], [[-5]]])
    assert len(pred) == 1
    assert len(pred[0]) == 1
    assert 5 < pred[0][0] < 7
