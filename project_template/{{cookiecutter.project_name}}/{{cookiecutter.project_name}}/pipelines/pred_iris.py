from chariots import MLMode, Pipeline
from chariots.nodes import Node

from {{cookiecutter.project_name}}.ops.model_ops.iris_pca import IrisPCA
from {{cookiecutter.project_name}}.ops.model_ops.iris_rf import IrisRF
from {{cookiecutter.project_name}}.ops.data_ops.from_array import FromArray


pred_iris = Pipeline(
    [
        Node(IrisPCA(MLMode.PREDICT), input_nodes=['__pipeline_input__'],
             output_nodes='x_pca'),
        Node(IrisRF(MLMode.PREDICT), input_nodes=['x_pca'],
             output_nodes='pred'),
        Node(FromArray(), input_nodes=['pred'], output_nodes=['__pipeline_output__'])
    ], 'pred_iris'
)
