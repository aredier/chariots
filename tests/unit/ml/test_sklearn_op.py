import pytest
import numpy as np
from sklearn.linear_model import LinearRegression

from chariots.backend import app
from chariots.backend.client import TestClient
from chariots.core import pipelines, nodes, versioning
from chariots.ml import sklearn_op, ml_op


@pytest.fixture
def LROp():
    class SKLROpInner(sklearn_op.SKSupervisedModel):
        model_class = versioning.VersionedField(LinearRegression, versioning.VersionType.MINOR)

    return SKLROpInner


def test_sk_training_pipeline(LROp, YOp, XTrainOp):
    train_pipe = pipelines.Pipeline([
        nodes.Node(XTrainOp(), output_nodes="x_train"),
        nodes.Node(YOp(), output_nodes="y_train"),
        nodes.Node(LROp(mode=ml_op.MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = pipelines.Pipeline([
        nodes.Node(LROp(mode=ml_op.MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="__pipeline_output__")
    ], "pred")

    runner = pipelines.SequentialRunner()
    runner.run(train_pipe)
    op_bytes = train_pipe.node_for_name["sklropinner"]._op.serialize()
    pred_pipe.node_for_name["sklropinner"]._op.load(op_bytes)
    response = runner.run(pred_pipe, pipeline_input=[[100], [101], [102]])

    for i, individual_value in enumerate(response):
        assert abs(101 + i - individual_value) < 1e-5

