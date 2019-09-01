from typing import Optional, List, Any

from chariots.base._base_serializer import BaseSerializer
from chariots.versioning._version import Version
from ._data_node import DataNode
from .._helpers.typing import InputNodes


class DataSavingNode(DataNode):
    """
    a node for loading data from a saver (that has to be attached after init)

    :param path: the path where to save the node
    :param serializer: the serializer corresponding to the format in which to save the data
    :param input_nodes: the input_nodes on which this node should be executed
    :param name: the name of the op
    """

    def __init__(self, serializer: BaseSerializer, path: str, input_nodes: Optional[InputNodes],
                 name: Optional[str] = None):
        super().__init__(serializer=serializer, path=path, input_nodes=input_nodes, name=name)

    @property
    def node_version(self) -> Version:
        """
        the version of the op this node represents
        """
        return Version()

    def execute(self, params: List[Any], runner: Optional["pipelines.AbstractRunner"] = None) -> Any:
        """
        executes the underlying op on params

        :param runner: present for cross-compatibility with parent class
        :param params: the data to save

        :return: the output of the op
        """
        if self._saver is None:
            raise ValueError("cannot save data without a saver")
        self._saver.save(self.serializer.serialize_object(params[0]), self.path)

    def __repr__(self):
        return "<DataLoadingNode of {}>".format(self.path)