from chariots.core import pipelines, nodes, saving
from chariots.ml import ml_op

from {{cookiecutter.project_name}}.ops.feature_ops.extract_x import ExtractX
from {{cookiecutter.project_name}}.ops.feature_ops.extract_y import ExtractY
from {{cookiecutter.project_name}}.ops.model_ops.iris_pca import IrisPCA
from {{cookiecutter.project_name}}.ops.model_ops.iris_rf import IrisRF


train_iris = pipelines.Pipeline(
    [
        nodes.DataLoadingNode(serializer=saving.CSVSerializer(),
                              path="iris.csv", output_node="iris_x_raw"),
        nodes.DataLoadingNode(serializer=saving.CSVSerializer(),
                              path="iris.csv", output_node="iris_y_raw"),
        nodes.Node(ExtractX(), input_nodes=["iris_x_raw"],
                   output_node="extracted_x"),
        nodes.Node(ExtractY(), input_nodes=["iris_y_raw"],
                   output_node="extracted_y"),
        nodes.Node(IrisPCA(ml_op.MLMode.FIT_PREDICT),
                   input_nodes=["extracted_x"], output_node="x_pca"),
        nodes.Node(IrisRF(ml_op.MLMode.FIT),
                   input_nodes=["x_pca", "extracted_y"])

    ], "train_iris"
)
