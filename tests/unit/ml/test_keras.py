"""module that tests the Keras integration"""
import numpy as np
from flaky import flaky
from tensorflow.keras import models, layers, optimizers, callbacks

from chariots.pipelines import Pipeline, PipelinesServer
from chariots.pipelines.nodes import Node
from chariots.pipelines.ops import BaseOp
from chariots.ml import MLMode
from chariots.ml.keras import KerasOp
from chariots.versioning import VersionedFieldDict, VersionType
from chariots.testing import TestPipelinesClient
from chariots._helpers.test_helpers import FromArray, ToArray, LinearDataSet, KerasLogistic


@flaky(5, 1)
def test_train_keras_pipeline(tmpdir, opstore_func):
    """tests using an op in training and testing"""

    train = Pipeline([
        Node(LinearDataSet(rows=10), output_nodes=['X', 'y']),
        Node(KerasLogistic(MLMode.FIT), input_nodes=['X', 'y'])
    ], 'train')

    pred = Pipeline([
        Node(ToArray(output_shape=(-1, 1)), input_nodes=['__pipeline_input__'], output_nodes='X'),
        Node(KerasLogistic(MLMode.PREDICT), input_nodes=['X'], output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], 'pred')
    my_app = PipelinesServer(app_pipelines=[train, pred],
                             op_store_client=opstore_func(tmpdir), import_name='my_app')
    client = TestPipelinesClient(my_app)
    client.call_pipeline(train)
    client.save_pipeline(train)
    client.load_pipeline(pred)

    pred = client.call_pipeline(pred, [[5]]).value
    assert len(pred) == 1
    assert len(pred[0]) == 1
    assert 5 < pred[0][0] < 7


class MultiDataSet(BaseOp):
    """Dataset op that has multiple inputs (to test keras' functional API)"""

    def execute(self):  # pylint: disable=arguments-differ
        return (
            [
                np.array([[i] for i in range(10) for _ in range(10)]),
                np.array([[-i] for i in range(10) for _ in range(10)])
            ],
            np.array([i + 1 for i in range(10) for _ in range(10)])
        )


class MultiInputKeras(KerasOp):
    """Keras based ML Op with multiple inputs"""
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


@flaky(5, 1)
def test_keras_multiple_datasets(tmpdir, opstore_func):
    """tests keras with a multi-input model (build using the functional API)"""

    class CreateInputs(BaseOp):
        """op that creates inputs for the pipeline"""

        def execute(self, input_data):  # pylint: disable=arguments-differ
            return [np.array(input_data[0]), np.array(input_data[1])]

    train = Pipeline([
        Node(MultiDataSet(), output_nodes=['X', 'y']),
        Node(MultiInputKeras(MLMode.FIT), input_nodes=['X', 'y'])
    ], 'train')

    pred = Pipeline([
        Node(CreateInputs(), input_nodes=['__pipeline_input__'], output_nodes='X'),
        Node(MultiInputKeras(MLMode.PREDICT), input_nodes=['X'], output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes='__pipeline_output__')
    ], 'pred')
    my_app = PipelinesServer(app_pipelines=[train, pred, ],
                             op_store_client=opstore_func(tmpdir), import_name='my_app')
    client = TestPipelinesClient(my_app)
    client.call_pipeline(train)
    client.save_pipeline(train)
    client.load_pipeline(pred)

    inputs = [[[5]], [[-5]]]
    pred = client.call_pipeline(pred, inputs).value
    assert len(pred) == 1
    for batch_predictions, batch_inputs in zip(pred, inputs):
        assert len(batch_predictions) == 1
        assert batch_inputs[0][0] < batch_predictions[0] < batch_inputs[0][0] + 2
