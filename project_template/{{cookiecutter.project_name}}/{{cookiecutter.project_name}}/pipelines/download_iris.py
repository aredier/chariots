import chariots.nodes._data_saving_node
import chariots.nodes._node
import chariots.serializers._csv_serialzer
from chariots.base import _pipelines, _base_nodes, _base_saver

from {{cookiecutter.project_name}}.ops.data_ops.download_iris import DownloadIris


download_iris = _pipelines.Pipeline(
    [
        chariots.nodes._node.Node(DownloadIris(), output_nodes="iris_df"),
        chariots.nodes._data_saving_node.DataSavingNode(serializer=chariots.serializers._csv_serialzer.CSVSerializer(),
                                                        path="iris.csv", input_nodes=["iris_df"])
    ], "download_iris"
)
