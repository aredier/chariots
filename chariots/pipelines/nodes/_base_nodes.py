"""abstract nodes of Chariots"""
from abc import abstractmethod, ABC
from enum import Enum

from typing import Any, Union, Optional, List, Text

from ... import versioning, errors, op_store
from ..._helpers.typing import SymbolicToRealMapping


class NodeReference:
    """referecne between an upstream and a downstream node in a pipeline"""

    def __init__(self, node: Union['BaseNode', 'ReservedNodes'], reference: Text):
        self.node = node
        if isinstance(reference, ReservedNodes):
            reference = reference.value
        if not isinstance(reference, str):
            raise TypeError('cannot reference with other than string')
        self.reference = reference

    def __repr__(self):
        return '<NodeReference {} of {}>'.format(self.reference, self.node.name)

    def __eq__(self, other):
        if not isinstance(other, NodeReference):
            raise TypeError('cannot compare NodeReference to {}'.format(type(other)))
        return self.node == other.node and self.reference == other.reference

    def __hash__(self):
        return hash((self.node, self.reference))


class BaseNode(ABC):
    """
    A node represents a step in a Pipeline. It is linked to one or several inputs and can produce one or several
    ouptuts:

    .. testsetup::

        >>> from chariots.pipelines import Pipeline
        >>> from chariots.pipelines.nodes import Node
        >>> from chariots.ml import MLMode
        >>> from chariots._helpers.doc_utils import IrisFullDataSet, PCAOp, LogisticOp

    .. doctest::

        >>> train_logistics = Pipeline([
        ...     Node(IrisFullDataSet(), output_nodes=["x", "y"]),
        ...     Node(PCAOp(MLMode.FIT_PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
        ... ], 'train_logistics')

    you can also link the first and/or the last node of your pipeline  to the pipeline input and output:

    .. doctest::

        >>> pred = Pipeline([
        ...     Node(IrisFullDataSet(),input_nodes=['__pipeline_input__'], output_nodes=["x"]),
        ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_nodes=['__pipeline_output__'])
        ... ], 'pred')

    Here we are showing the behavior of nodes using the Node subclass (used with ops).

    If you want to create your own Node you will need to define the

    - `node_version` property that gives the version of the node
    - `name` property
    - `execute` method that defines the execution behavior of your custom Node
    - `load_latest_version` that defines how to load the latest version of this node

    :param input_nodes: the input_nodes on which this node should be executed
    :param output_nodes: an optional symbolic name for the outputs of this node (to be used by downstream nodes in the
                         pipeline. If this node is the output of the pipeline use `__pipeline_output__` or
                         `ReservedNodes.pipeline_output`. If the output of the node should be split (for different
                         downstream nodes to consume) use a list
    """

    def __init__(self, input_nodes: Optional[List[Union[Text, 'BaseNode']]] = None,
                 output_nodes: Union[List[Text], Text] = None):
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
        """the different outputs of this nodes"""
        if self.output_nodes[0] is None:
            return [NodeReference(self, self.name)]
        return [NodeReference(self, reference) for reference in self.output_nodes]

    @property
    @abstractmethod
    def node_version(self) -> versioning.Version:
        """the version of this node"""

    @abstractmethod
    def execute(self, *params) -> Any:
        """
        executes the computation represented byt this node (loads/saves dataset for dataset nodes, executes underlyin op
        for `Node`

        :param params: the inputs provided by the `input_nodes`
        :return: the output(s) of the node
        """

    def replace_symbolic_references(self, symbolic_to_real_node: SymbolicToRealMapping) -> 'BaseNode':
        """
        replaces all the symbolic references of this node: if an input_node or output_node was defined with a string by
        the user, it will try to find the node represented by this string.

        :param symbolic_to_real_node: the mapping of all `NodeReference` found so far in the pipeline

        :raises ValueError: if a node with multiple outputs was used directly (object used rather than strings)

        :return: this node with all it's inputs and outputs as `NodeReferences` rather than strings
        """
        self.input_nodes = [self._ensure_node_is_real(node, symbolic_to_real_node) for node in self.input_nodes]
        return self

    @staticmethod
    def _ensure_node_is_real(node, symbolic_real_node_map: SymbolicToRealMapping) -> 'BaseNode':
        if isinstance(node, BaseNode):
            output_refs = node.output_references
            if not len(output_refs) == 1:
                raise ValueError('cannot use {} as input reference as it has {} output '
                                 'references'.format(node.name, len(output_refs)))
            ref = output_refs[0]
            return symbolic_real_node_map[ref.reference]
        return symbolic_real_node_map[node]

    @abstractmethod
    def load_latest_version(self, store_to_look_in: 'op_store.OpStoreClient') -> 'BaseNode':
        """
        reloads the latest available version of thid node by looking for all available versions in the OpStore

        :param store_to_look_in:  the store to look for new versions and eventually for bytes of serialized ops

        :return: this node once it has been loaded
        """

    def check_version_compatibility(self, upstream_node: 'BaseNode',
                                    store_to_look_in: 'op_store.OpStoreClient'):
        """
        checks that this node is compatible with a potentially new version of an upstream node`

        :param upstream_node: the upstream node to check for version compatibality with
        :param store_to_look_in: the op_store_client to look for valid relationships between this node and upstream
                                 versions

        :raises VersionError: when the two nodes are not compatible
        """
        validated_links = store_to_look_in.get_validated_links(self.name, upstream_node.name)
        if validated_links is None:
            return
        if upstream_node.node_version.major not in {version.major for version in validated_links}:
            raise errors.VersionError('cannot find a validated link from {} to {}'.format(upstream_node.name, self.name))

    @property
    def is_loadable(self) -> bool:
        """whether or not this node can be loaded (this is used by pipelined to know which nodes to load"""
        return False

    @property
    @abstractmethod
    def name(self) -> str:
        """the name of the node"""

    def persist(self, store: 'op_store.OpStoreClient',
                downstream_nodes: Optional[List['BaseNode']]) -> versioning.Version:
        """
        persists this nodes's data (usually this means saving the serialized bytes of the inner op of this node (for the
        `Node` class

        :param store: the store in which to store the node
        :param downstream_nodes: the node(s) that are going to accept the current version of this node as upstream
        """
        version = self.node_version
        if downstream_nodes is None:
            store.register_valid_link(downstream_op_name=None, upstream_op_name=self.name,
                                      upstream_op_version=version)
            return version
        for downstream_node in downstream_nodes:
            store.register_valid_link(downstream_op_name=downstream_node.name, upstream_op_name=self.name,
                                      upstream_op_version=version)
        return version

    @property
    def requires_runner(self) -> bool:
        """whether or not this node requires a runner to be executed (typically if the inner op is a pipelines)"""
        return False

    @property
    def require_saver(self) -> bool:
        """whether or not this node requires a saver to be executed this is usualy `True` by data nodes"""
        return False

    @abstractmethod
    def __repr__(self):
        pass


class ReservedNodes(Enum):
    """
    enum of reserved node names
    """

    pipeline_input = '__pipeline_input__'
    pipeline_output = '__pipeline_output__'

    @property
    def reference(self):
        """the output references of the reserved nodes"""
        return NodeReference(self, self.value)
