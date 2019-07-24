
import pytest
import numpy as np
from sklearn.linear_model import LinearRegression

from chariots.backend import app
from chariots.backend.app import PipelineResponse
from chariots.backend.client import TestClient
from chariots.core import pipelines, nodes, ops
from chariots.core.versioning import Version
from chariots.ml.ml_op import MLOp, MLMode

from .test_app_backend import post_app


@pytest.fixture
def XTrainOp():
    class XTrainOpInner(ops.AbstractOp):

        def __call__(self):
            return list(range(10))
    return XTrainOpInner


@pytest.fixture
def YOp():
    class YOpInner(ops.AbstractOp):

        def __call__(self):
            return list(range(1, 11))
    return YOpInner


@pytest.fixture
def LROp():
    class LROpInner(MLOp):

        def fit(self, x_train, y_train):
            self._model.fit(np.array(x_train).reshape(-1, 1), y_train)

        def predict(self, x_pred):
            if not isinstance(x_pred, list):
                x_pred = [x_pred]
            return int(self._model.predict(np.array(x_pred).reshape(-1, 1))[0])

        def _init_model(self):
            return LinearRegression()
    return LROpInner


def test_training_pipeline(LROp, YOp, XTrainOp):
    train_pipe = pipelines.Pipeline([
        nodes.Node(XTrainOp(), output_node="x_train"),
        nodes.Node(YOp(), output_node="y_train"),
        nodes.Node(LROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = pipelines.Pipeline([
        nodes.Node(LROp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_node="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[train_pipe, pred_pipe], path="/tmp/chariots", import_name="my_app")

    test_client = TestClient(my_app)
    pior_versions_train = test_client.pipeline_versions(train_pipe)
    pior_versions_pred = test_client.pipeline_versions(pred_pipe)
    test_client.request(train_pipe)
    test_client.save_pipeline(train_pipe)
    test_client.load_pipeline(pred_pipe)
    posterior_versions_train = test_client.pipeline_versions(train_pipe)
    posterior_versions_pred = test_client.pipeline_versions(pred_pipe)
    response = test_client.request(pred_pipe, pipeline_input=3)

    assert response.value == 4

    # testing verison uodate
    lrop_train = train_pipe.node_for_name["lropinner"]
    lrop_pred = pred_pipe.node_for_name["lropinner"]
    assert pior_versions_train[lrop_train]["node_version"] == pior_versions_pred[lrop_pred]["node_version"]
    assert posterior_versions_train[lrop_train]["node_version"] == posterior_versions_pred[lrop_pred]["node_version"]
    assert pior_versions_pred[lrop_pred]["node_version"] < posterior_versions_pred[lrop_pred]["node_version"]


