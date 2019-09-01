import pytest
from sklearn.linear_model import LinearRegression

from chariots import MLMode, Pipeline
import chariots.nodes._node
import chariots.runners._sequential_runner
import chariots.sklearn._sk_supervised_op
import chariots.versioning
import chariots.versioning._version_type
import chariots.versioning._versioned_field
from chariots.base import BaseMLOp
from chariots.sklearn import SKSupervisedOp


@pytest.fixture
def LROp():
    class SKLROpInner(SKSupervisedOp):
        model_class = chariots.versioning._versioned_field.VersionedField(
            LinearRegression, chariots.versioning._version_type.VersionType.MINOR
        )

    return SKLROpInner


def test_sk_training_pipeline(LROp, YOp, XTrainOp):
    train_pipe = Pipeline([
        chariots.nodes._node.Node(XTrainOp(), output_nodes="x_train"),
        chariots.nodes._node.Node(YOp(), output_nodes="y_train"),
        chariots.nodes._node.Node(LROp(mode=MLMode.FIT), input_nodes=["x_train", "y_train"])
    ], "train")
    pred_pipe = Pipeline([
        chariots.nodes._node.Node(LROp(mode=MLMode.PREDICT), input_nodes=["__pipeline_input__"],
                                  output_nodes="__pipeline_output__")
    ], "pred")

    runner = chariots.runners._sequential_runner.SequentialRunner()
    runner.run(train_pipe)
    op_bytes = train_pipe.node_for_name["sklropinner"]._op.serialize()
    pred_pipe.node_for_name["sklropinner"]._op.load(op_bytes)
    response = runner.run(pred_pipe, pipeline_input=[[100], [101], [102]])

    for i, individual_value in enumerate(response):
        assert abs(101 + i - individual_value) < 1e-5

