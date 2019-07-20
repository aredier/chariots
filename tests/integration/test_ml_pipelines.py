
import pytest
import numpy as np
from sklearn.linear_model import LinearRegression

from chariots.backend import app
from chariots.backend.app import PipelineResponse
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
            print(x_train, y_train)
            self._model.fit(np.array(x_train).reshape(-1, 1), y_train)

        def predict(self, x_pred):
            if not isinstance(x_pred, list):
                x_pred = [x_pred]
            return int(self._model.predict(np.array(x_pred).reshape(-1, 1))[0])

        def _init_model(self):
            return LinearRegression()
    return LROpInner


def test_rtaining_pipeline(LROp, YOp, XTrainOp):
    pipe = pipelines.Pipeline([
        nodes.Node(XTrainOp(), output_node="x_train"),
        nodes.Node(YOp(), output_node="y_train"),
        nodes.Node(LROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = pipelines.Pipeline([
        nodes.Node(LROp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_node="__pipeline_output__")
    ], "pred")
    my_app = app.Chariot(app_pipelines=[pipe, pred_pipe], path="/tmp/chariots", import_name="my_app")

    test_client = my_app.test_client()

    response_json = post_app(test_client, "/pipelines/train/main", )
    assert response_json["pipeline_output"] is None
    post_app(test_client, "/pipelines/train/save")
    post_app(test_client, "/pipelines/pred/load")
    response_json = post_app(test_client, "/pipelines/pred/main", data={"pipeline_input": 3})
    response = PipelineResponse(response_json["pipeline_output"],
                                {pred_pipe.node_for_name[node_name]: Version.parse(version_str)
                                 for node_name, version_str in response_json["versions"].items()})
    assert response.value == 4


