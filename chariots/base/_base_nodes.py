from abc import abstractmethod, ABC
from enum import Enum

from typing import Any, Union, Optional, List, Text

from chariots import _op_store
from chariots.versioning import Version
from chariots.errors import VersionError
from .._helpers.typing import SymbolicToRealMapping


class NodeReference:

    def __init__(self, node: Union["BaseNode", "ReservedNodes"], reference: Text):
        self.node = node
        if isinstance(reference, ReservedNodes):
            reference = reference.value
        if not isinstance(reference, str):
            raise TypeError("cannot reference with other than string")
        self.reference = reference

    def __repr__(self):
        return "<NodeReference {} of {}>".format(self.reference, self.node.name)

    def __eq__(self, other):
        if not isinstance(other, NodeReference):
            raise TypeError("cannot compare NodeReference to {}".format(type(other)))
        return self.node == other.node and self.reference == other.reference

    def __hash__(self):
        return hash((self.node, self.reference))


class BaseNode(ABC):
    """
    a Node handles the interaction of an op with other ops/nodes.
    It represents a slot in the pipeline.
    """

    def __init__(self, input_nodes: Optional[List[Union[Text, "BaseNode"]]] = None,
                 output_nodes: Union[List[Text], Text] = None):
        """
        :param input_nodes: the input_nodes on which this node should be executed
        :param output_nodes: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use `__pipeline_output__` or `ReservedNodes.pipeline_output`. If the output of the node
        should be split (for different downstream ops to consume) use a list
        """
        self.input_nodes = input_nodes or []
        if not isinstance(output_nodes, list):
            output_nodes = [output_nodes]
        if output_nodes is None:
            output_nodes = self.name
        self.output_nodes = output_nodes
        if self.output_nodes == ReservedNodes.pipeline_output:
            self.output_nodes = ReservedNodes.pipeline_output.value

    @property
    def output_references(self) -> List[NodeReference]:
        if self.output_nodes[0] is None:
            return [NodeReference(self, self.name)]
        return [NodeReference(self, reference) for reference in self.output_nodes]

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

    def replace_symbolic_references(self, symbolic_to_real_node: SymbolicToRealMapping) -> "BaseNode":
        """
        replaces symbolic references (input_nodes specified as strings) by the objects they reference

        :param symbolic_to_real_node: the mapping of nodes for their symbolic name
        :return: this node with it's symbolic inputs replaced
        """
        self.input_nodes = [self._ensure_node_is_real(node, symbolic_to_real_node) for node in self.input_nodes]
        return self

    @staticmethod
    def _ensure_node_is_real(node, symbolic_real_node_map: SymbolicToRealMapping) -> "BaseNode":
        if isinstance(node, BaseNode):
            output_refs = node.output_references
            if not len(output_refs) == 1:
                raise ValueError("cannot use {} as input reference as it has {} output "
                                 "references".format(node.name, len(output_refs)))
            ref = output_refs[0]
            return symbolic_real_node_map[ref.reference]
        return symbolic_real_node_map[node]

    @abstractmethod
    def load_latest_version(self, store_to_look_in: _op_store.OpStore) -> "BaseNode":
        """
        reloads the latest version of this op by looking into the available versions of the store
        :param store_to_look_in:  the store to look for new versions in
        :return:
        """

    def check_version_compatibility(self, upstream_node: "BaseNode", store_to_look_in: _op_store.OpStore):
        """
        checks that this node is compatible with a potentially new upstream

        :raises VersionError: When the nodes are not compatible

        :param upstream_node: the node to check the version for
        :param store_to_look_in: the op_store to look for previous relationships between the nodes in
        """
        validated_links = store_to_look_in.get_validated_links(self.name, upstream_node.name)
        if validated_links is None:
            return
        if upstream_node.node_version.major not in {version.major for version in validated_links}:
            raise VersionError("cannot find a validated link from {} to {}".format(upstream_node.name, self.name))

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

    def persist(self, store: _op_store.OpStore, downstream_nodes: Optional[List["BaseNode"]]) -> Version:
        """
        persists the inner op of the node in saver

        :param store: the store in which to store the node
        :param downstream_nodes: the node(s) that are going to accept this node downstrem
        """
        version = self.node_version
        if downstream_nodes is None:
            store.register_valid_link(downstream_op=None, upstream_op=self.name,
                                      upstream_op_version=version)
            return version
        for downstream_node in downstream_nodes:
            store.register_valid_link(downstream_op=downstream_node.name, upstream_op=self.name,
                                      upstream_op_version=version)
        return version

    @property
    def requires_runner(self) -> bool:
        """
        whether or not this node requires q runner to be executed
        (typically if the inner op is a pipelines)

        :return: bool
        """
        return False

    @property
    def require_saver(self) -> bool:
        """
        whether or not this node requires a saver to be executed
        :return: bool
        """
        return False

    @abstractmethod
    def __repr__(self):
        pass


class ReservedNodes(Enum):
    """
    enum of reserved node names
    """

    pipeline_input = "__pipeline_input__"
    pipeline_output = "__pipeline_output__"

    @property
    def reference(self):
        return NodeReference(self, self.value)
