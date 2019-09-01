import chariots._ml_mode
import chariots._pipeline
import chariots.nodes._data_loading_node
import chariots.nodes._node
import chariots.serializers._csv_serialzer
from chariots.base import _pipelines, _base_nodes, _base_saver
from chariots._ml import ml_op

from {{cookiecutter.project_name}}.ops.feature_ops.extract_x import ExtractX
from {{cookiecutter.project_name}}.ops.feature_ops.extract_y import ExtractY
from {{cookiecutter.project_name}}.ops.model_ops.iris_pca import IrisPCA
from {{cookiecutter.project_name}}.ops.model_ops.iris_rf import IrisRF


train_iris = chariots._pipeline.Pipeline(
    [
        chariots.nodes._data_loading_node.DataLoadingNode(serializer=chariots.serializers._csv_serialzer.CSVSerializer(),
                                                          path="iris.csv", output_nodes="iris_x_raw"),
        chariots.nodes._data_loading_node.DataLoadingNode(serializer=chariots.serializers._csv_serialzer.CSVSerializer(),
                                                          path="iris.csv", output_nodes="iris_y_raw"),
        chariots.nodes._node.Node(ExtractX(), input_nodes=["iris_x_raw"],
                                  output_nodes="extracted_x"),
        chariots.nodes._node.Node(ExtractY(), input_nodes=["iris_y_raw"],
                                  output_nodes="extracted_y"),
        chariots.nodes._node.Node(IrisPCA(chariots._ml_mode.MLMode.FIT_PREDICT),
                                  input_nodes=["extracted_x"], output_nodes="x_pca"),
        chariots.nodes._node.Node(IrisRF(chariots._ml_mode.MLMode.FIT),
                                  input_nodes=["x_pca", "extracted_y"])

    ], "train_iris"
)
