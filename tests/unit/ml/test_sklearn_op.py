import pytest
from sklearn.linear_model import LinearRegression

from chariots import MLMode, Pipeline
from chariots.nodes import Node
from chariots.runners import SequentialRunner
from chariots.sklearn import SKSupervisedOp
from chariots.versioning import VersionType, VersionedField


@pytest.fixture
def LROp():
    class SKLROpInner(SKSupervisedOp):
        model_class = VersionedField(
            LinearRegression, VersionType.MINOR
        )

    return SKLROpInner


def test_sk_training_pipeline(LROp, YOp, XTrainOp):
    train_pipe = Pipeline([
        Node(XTrainOp(), output_nodes="x_train"),
        Node(YOp(), output_nodes="y_train"),
        Node(LROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = Pipeline([
        Node(LROp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"],
             output_nodes="__pipeline_output__")
    ], "pred")

    runner = SequentialRunner()
    runner.run(train_pipe)
    op_bytes = train_pipe.node_for_name["sklropinner"]._op.serialize()
    pred_pipe.node_for_name["sklropinner"]._op.load(op_bytes)
    response = runner.run(pred_pipe, pipeline_input=[[100], [101], [102]])

    for i, individual_value in enumerate(response):
        assert abs(101 + i - individual_value) < 1e-5
