import json
import os

from chariots.core.nodes import DataLoadingNode, DataSavingNode
from chariots.core.saving import JSONSerializer, FileSaver


def test_loading_node(tmpdir):
    json_file_path = "mhy_file"
    with open(os.path.join(tmpdir, json_file_path), "w") as file:
        json.dump({"foo": 1}, file)

    node = DataLoadingNode(serializer=JSONSerializer(), path=json_file_path)
    saver = FileSaver(tmpdir)
    node.attach_saver(saver)

    assert node.execute() == {"foo": 1}


def test_saving_node(tmpdir):
    json_file_path = "mhy_file"

    node = DataSavingNode(serializer=JSONSerializer(), path=json_file_path, input_nodes=["_"])
    saver = FileSaver(tmpdir)
    node.attach_saver(saver)
    node.execute({"foo": 2})

    with open(os.path.join(tmpdir, json_file_path), "r") as file:
        assert json.load(file) == {"foo": 2}

