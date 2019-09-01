import chariots._ml_mode
import chariots._pipeline
import chariots.nodes._node
from chariots.base import _pipelines, _base_nodes
from chariots._ml import ml_op

from {{cookiecutter.project_name}}.ops.model_ops.iris_pca import IrisPCA
from {{cookiecutter.project_name}}.ops.model_ops.iris_rf import IrisRF


pred_iris = chariots._pipeline.Pipeline(
    [
        chariots.nodes._node.Node(IrisPCA(chariots._ml_mode.MLMode.PREDICT),
                                  input_nodes=["__pipeline_input__"], output_nodes="x_pca"),
        chariots.nodes._node.Node(IrisRF(chariots._ml_mode.MLMode.PREDICT), input_nodes=["x_pca"],
                                  output_nodes="__pipeline_output__")

    ], "pred_iris"
)
