from hashlib import sha1
from typing import Optional, List, Any

from chariots.base._base_serializer import BaseSerializer
from chariots.versioning._version import Version
from ._data_node import DataNode


class DataLoadingNode(DataNode):
    """
    a node for loading data from a saver (that has to be attached after init)

    :param serializer: the serializer to use to load the dat
    :param path: the path to load the data from
    :param output_nodes: an optional symbolic name for the node to be called by other node. If this node is the
                         output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
    :param name: the name of the op
    """

    def __init__(self, serializer: BaseSerializer, path: str, output_nodes=None, name: Optional[str] = None):
        super().__init__(serializer=serializer, path=path, output_nodes=output_nodes, name=name)

    @property
    def node_version(self) -> Version:
        """
        the version of the op this node represents
        """
        if self._saver is None:
            raise ValueError("cannot get the version of a data op without a saver")
        version = Version()
        file_hash = sha1(self._saver.load(self.path)).hexdigest()
        version.update_major(file_hash.encode("utf-8"))
        return version

    def execute(self, params: List[Any], runner: Optional["pipelines.AbstractRunner"] = None) -> Any:
        """
        executes the underlying op on params

        :param params: the inputs of the node
        :param runner: present for cross-compatibility with parent class

        :return: the output of the op
        """

        if self._saver is None:
            raise ValueError("cannot load data without a saver")
        return self.serializer.deserialize_object(self._saver.load(self.path))

    def __repr__(self):
        return "<DataLoadingNode of {}>".format(self.path)