import os
from abc import ABCMeta, abstractmethod
from hashlib import sha1
from typing import Optional, Any

import chariots._op_store
from chariots.base import BaseNode, BaseSaver, BaseSerializer
from chariots.versioning import Version
from .._helpers.constants import DATA_PATH
from .._helpers.typing import InputNodes


class DataNode(BaseNode, metaclass=ABCMeta):

    def __init__(self, serializer: BaseSerializer, path: str, input_nodes: Optional[InputNodes] = None,
                 output_nodes=None, name: Optional[str] = None):
        """
        :param serializer: the serializer to use to load the dat
        :param path: the path to load the data from
        :param input_nodes: the input_nodes on which this node should be executed
        :param output_nodes: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
        :param name: the name of the op
        """

        super().__init__(input_nodes=input_nodes, output_nodes=output_nodes)
        self.path = os.path.join(DATA_PATH, path)
        self.serializer = serializer
        self._name = name
        self._saver = None

    def load_latest_version(self, store_to_look_in: chariots._op_store.OpStore) -> BaseNode:
        """
        reloads the latest version of this op by looking into the available versions of the store
        :param store_to_look_in:  the store to look for new versions in
        :return:
        """
        return self

    def attach_saver(self, saver: BaseSaver):
        """
        attach a saver to the op, this is the entry point for the Chariot App to inject it's saver to the Dat Op

        :param saver: the saver to use
        """
        self._saver = saver

    @property
    def name(self) -> str:
        """
        the name of the node

        :return: the string of the name
        """
        return self._name or self.path.split("/")[-1].split(".")[0]

    @property
    @abstractmethod
    def node_version(self) -> Version:
        """
        the version of the op this node represents
        """
        if self._saver is None:
            raise ValueError("cannot get the version of a data op without a saver")
        version = Version()
        file_hash = sha1(self._saver.load(self.path)).hexdigest()
        version.update_major(file_hash)
        return version

    @abstractmethod
    def execute(self, *params) -> Any:
        """
        executes the underlying op on params

        :param params: the inputs of the underlying op
        :return: the output of the op
        """

        if self._saver is None:
            raise ValueError("cannot load data without a saver")
        return self.serializer.deserialize_object(self._saver.load(self.path))

    @property
    def require_saver(self) -> bool:
        return True

    @abstractmethod
    def __repr__(self):
        return "<DataLoadingNode of {}>".format(self.path)
