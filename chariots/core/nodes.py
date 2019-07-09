import os
from abc import abstractmethod, ABC
from enum import Enum

from typing import Any, List, Text, Union, Mapping

from chariots.core.ops import AbstractOp, OPS_PATH, LoadableOp
from chariots.core.pipelines import Pipeline, ReservedNodes
from chariots.core.saving import Saver
from chariots.core.versioning import Version
from chariots.helpers.typing import SymbolicToRealMapping


class AbstractNode(ABC):
    """
    a Node handles the interaction of an op with other ops/nodes.
    It represents a slot in the pipeline.
    """

    def __init__(self, input_nodes=None, output_node=None):
        """
        :param input_nodes: the input_nodes on which this node should be executed
        :param output_node: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
        """
        self.input_nodes = input_nodes or []
        self.output_node = output_node
        if self.output_node == ReservedNodes.pipeline_output:
            self.output_node = ReservedNodes.pipeline_output.value

    @property
    @abstractmethod
    def node_version(self) -> Version:
        """
        the version of the op this node represents
        """
        pass

    @abstractmethod
    def execute(self, *params) -> Any:
        """
        executes the underlying op on params

        :param params: the inputs of the underlying op
        :return: the output of the op
        """
        pass

    def replace_symbolic_references(self, symbolic_to_real_node: SymbolicToRealMapping) -> "AbstractNode":
        """
        replaces symbolic references (input_nodes specified as strings) by the objects they reference

        :param symbolic_to_real_node: the mapping of nodes for their symbolic name
        :return: this node with it's symbolic inputs replaced
        """
        self.input_nodes = [self._ensure_node_is_real(node, symbolic_to_real_node) for node in self.input_nodes]
        return self

    @staticmethod
    def _ensure_node_is_real(node, symbolic_real_node_map: SymbolicToRealMapping) -> "AbstractNode":
        if isinstance(node, str):
            return symbolic_real_node_map[node]
        return node

    def get_version_with_ancestry(self,
                                  ancestry_versions: Mapping[Union["AbstractNode", "ReservedNodes"], Version]) -> Version:
        """
        adds this node's version to those of it's input (themselves computed on their ancestry)

        :param ancestry_versions: a mapping with a version for each op which's version with ancestry has been computed
        :return: the resulting version
        """
        if not self.input_nodes:
            return self.node_version
        return self.node_version + sum((ancestry_versions[input_node] for input_node in self.input_nodes), Version())

    def check_version(self, persisted_versions: Mapping["AbstractNode", List[Version]],
                      current_versions: Mapping["AbstractNode", Version]):
        """
        checks the current pipeline version (with ancestry) against the persisted version

        :param persisted_versions: the persisted versions of loadable ops in the pipeline
        :param current_versions: the current versions of the ops (as submitted in the pipeline)
        """
        current_version = current_versions[self]
        last_loaded_version = max(persisted_versions[self])
        if current_version > last_loaded_version and current_version.major != last_loaded_version.major:
            raise ValueError("trying to load incompatible version")

    def load(self, saver: Saver, node_path: Text) -> "AbstractNode":
        """
        loads the op the node as persisted in node path
        raises ValueError if the node is not loadable

        :param saver: the saver to load the op from
        :param node_path: the path the node as persisted at
        :return: the loaded node
        """
        if not self.is_loadable:
            raise ValueError("trying to load a non loadable node")
        raise NotImplementedError("loading is not implemented for {} nodes".format(type(self)))

    @property
    def is_loadable(self) -> bool:
        """
        :return: whether or not this node and its inner op can be loaded
        """
        return False

    @property
    @abstractmethod
    def name(self) -> str:
        """
        the name of the node

        :return: the string of the name
        """

    def persist(self, saver: Saver, pipeline_version: Version):
        """
        persists the inner op of the node in saver

        :param pipeline_version: the pipeline version of the op (including ancestry)
        :param saver: the saver to save the op in
        """
        if not self.is_loadable:
            raise ValueError("trying to save a non savable/loadable op")
        raise NotImplementedError("persisting is not implemented for {} nodes".format(type(self)))

    @property
    def requires_runner(self) -> bool:
        """
        whether or not this node requires q runner to be executed
        (typically if the inner op is a pipelines)

        :return: bool
        """
        return False

    @abstractmethod
    def __repr__(self):
        pass


class Node(AbstractNode):
    """
    a Node handles the interaction of an op with other ops/nodes.
    It represents a slot in the pipeline.
    """

    def __init__(self, op: AbstractOp, input_nodes=None, output_node=None):
        """
        :param op: the op this Node wraps
        :param input_nodes: the input_nodes on which this node should be executed
        :param output_node: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
        """
        self._op = op
        super().__init__(input_nodes=input_nodes, output_node=output_node)

    @property
    def node_version(self) -> Version:
        """
        the version of the op this node represents
        """
        return self._op.__version__

    @property
    def has_symbolic_references(self) -> bool:
        """
        whether or not this node has symbolic references in its input
        """
        return any(isinstance(node, str) for node in self.input_nodes)

    def execute(self, *params) -> Any:
        """
        executes the underlying op on params

        :param params: the inputs of the underlying op
        :return: the output of the op
        """
        res = self._op(*params)
        return res

    def load(self, saver: Saver, node_path: Text) -> "Node":
        """
        loads the op the node as persisted in node path
        raises ValueError if the node is not loadable

        :param saver: the saver to load the op from
        :param node_path: the path the node as persisted at
        :return: the loaded node
        """
        if not self.is_loadable:
            raise ValueError("trying to load a non loadable node")
        if isinstance(self._op, Pipeline):
            self._op.load(saver)
            return self
        op_bytes = saver.load(node_path)
        self._op.load(op_bytes)
        return self

    @property
    def is_loadable(self) -> bool:
        """
        :return: whether or not this node and its inner op can be loaded
        """
        return isinstance(self._op, (LoadableOp, Pipeline))

    @property
    def name(self) -> str:
        """
        the name of the node

        :return: the string of the name
        """
        return self._op.name

    def persist(self, saver: Saver, pipeline_version: Version):
        """
        persists the inner op of the node in saver

        :param pipeline_version: the pipeline version of the op (including ancestry)
        :param saver: the saver to save the op in
        """
        if not self.is_loadable:
            raise ValueError("trying to save a non savable/loadable op")
        if isinstance(self._op, Pipeline):
            return self._op.save(saver)
        op_bytes = self._op.serialize()
        saver.save(op_bytes, os.path.join(OPS_PATH, self.name, str(pipeline_version)))

    @property
    def requires_runner(self) -> bool:
        """
        whether or not this node requires q runner to be executed
        (typically if the inner op is a pipelines)

        :return: bool
        """
        return isinstance(self._op, Pipeline)

    def __repr__(self):
        return "<Node of {} with inputs {} and output {}>".format(self._op.name, self.input_nodes, self.output_node)
