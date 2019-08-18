from chariots.core import pipelines, nodes, saving

from {{cookiecutter.project_name}}.ops.data_ops.download_iris import DownloadIris


download_iris = pipelines.Pipeline(
    [
        nodes.Node(DownloadIris(), output_node="iris_df"),
        nodes.DataSavingNode(serializer=saving.CSVSerializer(), path="iris.csv",
                             input_nodes=["iris_df"])
    ], "download_iris"
)