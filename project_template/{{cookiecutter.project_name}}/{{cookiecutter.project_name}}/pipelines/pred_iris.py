from chariots import MLMode, Pipeline
from chariots.nodes import Node

from {{cookiecutter.project_name}}.ops.model_ops.iris_pca import IrisPCA
from {{cookiecutter.project_name}}.ops.model_ops.iris_rf import IrisRF


pred_iris = Pipeline(
    [
        Node(IrisPCA(MLMode.PREDICT), input_nodes=['__pipeline_input__'],
             output_nodes='x_pca'),
        Node(IrisRF(MLMode.PREDICT), input_nodes=['x_pca'],
             output_nodes='__pipeline_output__')
    ], 'pred_iris'
)
