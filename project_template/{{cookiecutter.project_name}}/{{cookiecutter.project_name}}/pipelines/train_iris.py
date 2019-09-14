from chariots import MLMode, Pipeline
from chariots.nodes import DataLoadingNode, Node
from chariots.serializers import CSVSerializer

from {{cookiecutter.project_name}}.ops.feature_ops.x_y_split import XYSplit
from {{cookiecutter.project_name}}.ops.model_ops.iris_pca import IrisPCA
from {{cookiecutter.project_name}}.ops.model_ops.iris_rf import IrisRF


train_iris = Pipeline(
    [
        DataLoadingNode(serializer=CSVSerializer(), path="iris.csv",
                        output_nodes="iris"),
        Node(XYSplit(), input_nodes=["iris"], output_nodes=["raw_X", "y"]),
        Node(IrisPCA(MLMode.FIT_PREDICT), input_nodes=["raw_X"],
             output_nodes="pca_X"),
        Node(IrisRF(MLMode.FIT), input_nodes=["pca_X", "y"])
    ], "train_iris"
)
