import json
import os

from chariots.nodes._data_saving_node import DataSavingNode
from chariots.nodes._data_loading_node import DataLoadingNode
from chariots.serializers._json_serializer import JSONSerializer
from chariots.savers._file_saver import FileSaver


def test_loading_node(tmpdir):
    json_file_path = "m_file"
    os.makedirs(os.path.join(str(tmpdir), "data"), exist_ok=True)
    with open(os.path.join(str(tmpdir), "data", json_file_path), "w") as file:
        json.dump({"foo": 1}, file)

    node = DataLoadingNode(serializer=JSONSerializer(), path=json_file_path)
    saver = FileSaver(str(tmpdir))
    node.attach_saver(saver)

    assert node.execute([]) == {"foo": 1}


def test_saving_node(tmpdir):
    json_file_path = "my_file"

    node = DataSavingNode(serializer=JSONSerializer(), path=json_file_path, input_nodes=["_"])
    saver = FileSaver(str(tmpdir))
    node.attach_saver(saver)
    node.execute([{"foo": 2}])

    with open(os.path.join(str(tmpdir), "data", json_file_path), "r") as file:
        assert json.load(file) == {"foo": 2}

