from chariots.core import pipelines, nodes, saving
from chariots.ml import ml_op

from {{cookiecutter.project_name}}.ops.model_ops.iris_pca import IrisPCA
from {{cookiecutter.project_name}}.ops.model_ops.iris_rf import IrisRF


pred_iris = pipelines.Pipeline(
    [
        nodes.Node(IrisPCA(ml_op.MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_node="x_pca"),
        nodes.Node(IrisRF(ml_op.MLMode.PREDICT), input_nodes=["x_pca"], output_node="__pipeline_output__")

    ], "pred_iris"
)
