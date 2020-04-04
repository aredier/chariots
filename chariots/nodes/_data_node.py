"""base class for all the data nodes"""
import os
from abc import ABCMeta, abstractmethod
from hashlib import sha1
from typing import Optional, Any

import chariots.op_store._op_store
from chariots.base import BaseNode, BaseSaver, BaseSerializer
from chariots.versioning import Version
from .._helpers.constants import DATA_PATH
from .._helpers.typing import InputNodes


class DataNode(BaseNode, metaclass=ABCMeta):
    """
    DataNodes are used to serialize/deserialize the datasets/outputs that you need to use in your pipelines

    To use a `DataNode`, you need to have a :doc:`saver<./chariots.savers>` attached to the data nodes. you can either
    do it at init or using the `attach_saver` method

    :param saver: the saver to use for loading or saving data (if not specified at init, you can use the
                  `attach_saver` method
    :param serializer: the serializer to use to load the dat
    :param path: the path to load the data from
    :param input_nodes: the input_nodes on which this node should be executed
    :param output_nodes: an optional symbolic name for the node to be called by other node. If this node is the
                         output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
    :param name: the name of the op
    """

    def __init__(self, serializer: BaseSerializer, path: str,  # pylint: disable=too-many-arguments
                 input_nodes: Optional[InputNodes] = None,
                 output_nodes=None, name: Optional[str] = None, saver: Optional[BaseSaver] = None):

        super().__init__(input_nodes=input_nodes, output_nodes=output_nodes)
        self.path = os.path.join(DATA_PATH, path)
        self.serializer = serializer
        self._name = name
        self._saver = saver

    def load_latest_version(
            self, store_to_look_in: chariots.op_store._op_store.OpStore) -> BaseNode:  # pylint: disable=protected-access
        return self

    def attach_saver(self, saver: BaseSaver):
        """
        method used to attach a saver to this op. This needs to be done before the op is executed in order for it to
        know where to save/load the serialized/deserialized data
        """
        self._saver = saver

    @property
    def name(self) -> str:
        return self._name or self.path.split('/')[-1].split('.')[0]

    @property
    def node_version(self) -> Version:
        if self._saver is None:
            raise ValueError('cannot get the version of a data op without a saver')
        version = Version()
        file_hash = sha1(self._saver.load(self.path)).hexdigest()
        version.update_major(file_hash.encode('utf-8'))
        return version

    @abstractmethod
    def execute(self, *params) -> Any:

        if self._saver is None:
            raise ValueError('cannot load data without a saver')
        return self.serializer.deserialize_object(self._saver.load(self.path))

    @property
    def require_saver(self) -> bool:
        """set to True has this op cannot be executed by a pipeline before a saver has been attached to it."""
        return True

    @abstractmethod
    def __repr__(self):
        return '<DataLoadingNode of {}>'.format(self.path)
