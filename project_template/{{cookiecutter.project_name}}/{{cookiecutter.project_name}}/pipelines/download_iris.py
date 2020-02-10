from chariots import Pipeline
from chariots.nodes import DataSavingNode, Node
from chariots.serializers import CSVSerializer

from {{cookiecutter.project_name}}.ops.data_ops.download_iris import DownloadIris


download_iris = Pipeline(
    [
        Node(DownloadIris(), output_nodes='iris_df'),
        DataSavingNode(serializer=CSVSerializer(), path='iris.csv',
                       input_nodes=['iris_df'])
    ], 'download_iris'
)
