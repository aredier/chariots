from chariots import MLMode, Pipeline
from chariots.nodes import DataLoadingNode, Node
from chariots.serializers import CSVSerializer

from {{cookiecutter.project_name}}.ops.feature_ops.extract_x import ExtractX
from {{cookiecutter.project_name}}.ops.feature_ops.extract_y import ExtractY
from {{cookiecutter.project_name}}.ops.model_ops.iris_pca import IrisPCA
from {{cookiecutter.project_name}}.ops.model_ops.iris_rf import IrisRF


train_iris = Pipeline(
    [
        DataLoadingNode(serializer=CSVSerializer(), path="iris.csv",
                        output_nodes="iris_x_raw"),
        DataLoadingNode(serializer=CSVSerializer(), path="iris.csv",
                        output_nodes="iris_y_raw"),
        Node(ExtractX(), input_nodes=["iris_x_raw"], output_nodes="extracted_x"),
        Node(ExtractY(), input_nodes=["iris_y_raw"], output_nodes="extracted_y"),
        Node(IrisPCA(MLMode.FIT_PREDICT), input_nodes=["extracted_x"],
             output_nodes="x_pca"),
        Node(IrisRF(MLMode.FIT), input_nodes=["x_pca", "extracted_y"])

    ], "train_iris"
)
