import os
from hashlib import sha1
from abc import abstractmethod, ABC, ABCMeta

from typing import Any, Union, Optional, List, Text

from chariots.constants import DATA_PATH
from chariots.core.ops import AbstractOp, LoadableOp
from chariots.core import pipelines, op_store
from chariots.core.saving import Saver, Serializer
from chariots.core.versioning import Version
from chariots.helpers.errors import VersionError
from chariots.helpers.typing import SymbolicToRealMapping, InputNodes


class NodeReference:

    def __init__(self, node: Union["AbstractNode", "pipelines.ReservedNodes"], reference: Text):
        self.node = node
        if isinstance(reference, pipelines.ReservedNodes):
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


class AbstractNode(ABC):
    """
    a Node handles the interaction of an op with other ops/nodes.
    It represents a slot in the pipeline.
    """

    def __init__(self, input_nodes: Optional[List[Union[Text, "AbstractNode"]]] = None,
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
        if self.output_nodes == pipelines.ReservedNodes.pipeline_output:
            self.output_nodes = pipelines.ReservedNodes.pipeline_output.value

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
        if isinstance(node, AbstractNode):
            output_refs = node.output_references
            if not len(output_refs) == 1:
                raise ValueError("cannot use {} as input reference as it has {} output "
                                 "references".format(node.name, len(output_refs)))
            ref = output_refs[0]
            return symbolic_real_node_map[ref.reference]
        return symbolic_real_node_map[node]

    @abstractmethod
    def load_latest_version(self, store_to_look_in: op_store.OpStore) -> "AbstractNode":
        """
        reloads the latest version of this op by looking into the available versions of the store
        :param store_to_look_in:  the store to look for new versions in
        :return:
        """

    def check_version_compatibility(self, upstream_node: "AbstractNode", store_to_look_in: op_store.OpStore):
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

    def persist(self, store: op_store.OpStore, downstream_nodes: Optional[List["AbstractNode"]]) -> Version:
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


class Node(AbstractNode):
    """
    a Node handles the interaction of an op with other ops/nodes.
    It represents a slot in the pipeline.
    """

    def __init__(self, op: AbstractOp, input_nodes: Optional[List[Union[Text, "AbstractNode"]]] = None,
                 output_nodes: Union[List[Union[Text, "AbstractNode"]], Text, "AbstractNode"] = None):
        """
        :param op: the op this Node wraps
        :param input_nodes: the input_nodes on which this node should be executed
        :param output_nodes: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use `__pipeline_output__` or `ReservedNodes.pipeline_output`. If the output of the node
        should be split (for different downstream ops to consume) use a list
        """
        self._op = op
        super().__init__(input_nodes=input_nodes, output_nodes=output_nodes)

    @property
    def node_version(self) -> Version:
        """
        the version of the op this node represents
        """
        return self._op.op_version

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
        res = self._op.execute(*params)
        return res

    def load_latest_version(self, store_to_look_in: op_store.OpStore) -> "AbstractNode":
        """
        reloads the latest version of this op by looking into the available versions of the store
        :param store_to_look_in:  the store to look for new versions in
        :return:
        """
        if not self.is_loadable:
            return self
        if isinstance(self._op, pipelines.Pipeline):
            self._op.load(store_to_look_in)
            return self
        all_versions = store_to_look_in.get_all_verisons_of_op(self._op)
        # if no node has been saved we return None as the pipeline will need to register this Op
        # we also save this version as is (untrained for instance) so that it is not registered as new later
        if all_versions is None:
            return None

        # if the node is newer than persisted, we keep the in memory version
        if self.node_version.major not in {version.major for version in all_versions}:
            return self

        relevant_version = max(all_versions)
        self._op.load(store_to_look_in.get_op_bytes_for_version(self._op, relevant_version))
        return self

    def check_version_compatibility(self, upstream_node: "AbstractNode", store_to_look_in: op_store.OpStore):
        if self._op.allow_version_change:
            return
        super().check_version_compatibility(upstream_node, store_to_look_in)

    @property
    def is_loadable(self) -> bool:
        """
        :return: whether or not this node and its inner op can be loaded
        """
        return isinstance(self._op, (LoadableOp, pipelines.Pipeline))

    @property
    def name(self) -> str:
        """
        the name of the node

        :return: the string of the name
        """
        return self._op.name

    def persist(self, store: op_store.OpStore, downstream_nodes: Optional[List["AbstractNode"]]) -> Optional[Version]:
        """
        persists the inner op of the node in saver

        :param store: the store in which to store the node
        :param downstream_nodes: the node(s) that are going to accept this node downstrem
        """

        version = super().persist(store, downstream_nodes)
        if not self.is_loadable:
            return
        if isinstance(self._op, pipelines.Pipeline):
            return self._op.save(store)
        store.save_op_bytes(self._op, version, op_bytes=self._op.serialize())
        return version

    @property
    def requires_runner(self) -> bool:
        """
        whether or not this node requires q runner to be executed
        (typically if the inner op is a pipelines)

        :return: bool
        """
        return isinstance(self._op, pipelines.Pipeline)

    def __repr__(self):
        return "<Node of {} with inputs {} and output {}>".format(self._op.name, self.input_nodes, self.output_nodes)


class DataNode(AbstractNode, metaclass=ABCMeta):

    def __init__(self, serializer: Serializer, path: str, input_nodes: Optional[InputNodes] = None, output_nodes=None,
                 name: Optional[str] = None):
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

    def load_latest_version(self, store_to_look_in: op_store.OpStore) -> "AbstractNode":
        """
        reloads the latest version of this op by looking into the available versions of the store
        :param store_to_look_in:  the store to look for new versions in
        :return:
        """
        return self

    def attach_saver(self, saver: Saver):
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


class DataLoadingNode(DataNode):
    """
    a node for loading data from a saver (that has to be attached after init)
    """

    def __init__(self, serializer: Serializer, path: str, output_nodes=None, name: Optional[str] = None):
        """
        :param serializer: the serializer to use to load the dat
        :param path: the path to load the data from
        :param output_nodes: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
        :param name: the name of the op
        """
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

    def execute(self, *params) -> Any:
        """
        executes the underlying op on params

        :param params: the inputs of the underlying op
        :return: the output of the op
        """

        if self._saver is None:
            raise ValueError("cannot load data without a saver")
        return self.serializer.deserialize_object(self._saver.load(self.path))

    def __repr__(self):
        return "<DataLoadingNode of {}>".format(self.path)


class DataSavingNode(DataNode):
    """
    a node for loading data from a saver (that has to be attached after init)
    """

    def __init__(self, serializer: Serializer,  path: str, input_nodes: Optional[InputNodes],
                 name: Optional[str] = None):
        """
        :param path: the path where to save the node
        :param serializer: the serializer corresponding to the format in which to save the data
        :param input_nodes: the input_nodes on which this node should be executed
        :param name: the name of the op
        """
        super().__init__(serializer=serializer, path=path, input_nodes=input_nodes, name=name)

    @property
    def node_version(self) -> Version:
        """
        the version of the op this node represents
        """
        return Version()

    def execute(self, data_to_serialize) -> Any:
        """
        executes the underlying op on params

        :param data_to_serialize: the data to save

        :return: the output of the op
        """
        if self._saver is None:
            raise ValueError("cannot save data without a saver")
        self._saver.save(self.serializer.serialize_object(data_to_serialize), self.path)

    def __repr__(self):
        return "<DataLoadingNode of {}>".format(self.path)
