from hashlib import sha1
from abc import abstractmethod, ABC, ABCMeta

from typing import Any, Union, Mapping, Optional

from chariots.core.ops import AbstractOp, LoadableOp
from chariots.core import pipelines
from chariots.core.saving import Saver, Serializer
from chariots.core.versioning import Version
from chariots.helpers.errors import VersionError
from chariots.helpers.typing import SymbolicToRealMapping, InputNodes


class AbstractNode(ABC):
    """
    a Node handles the interaction of an op with other ops/nodes.
    It represents a slot in the pipeline.
    """

    def __init__(self, input_nodes=None, output_node=None):
        """
        :param input_nodes: the input_nodes on which this node should be executed
        :param output_node: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use "pipeline_output" or `pipelines.ReservedNodes.pipeline_output`
        """
        self.input_nodes = input_nodes or []
        self.output_node = output_node
        if self.output_node == pipelines.ReservedNodes.pipeline_output:
            self.output_node = pipelines.ReservedNodes.pipeline_output.value

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

    def get_version_with_ancestry(
            self, ancestry_versions: Mapping[Union["AbstractNode", "pipelines.ReservedNodes"], Version]
    ) -> Version:
        """
        adds this node's version to those of it's input (themselves computed on their ancestry)

        :param ancestry_versions: a mapping with a version for each op which's version with ancestry has been computed
        :return: the resulting version
        """
        if not self.input_nodes:
            return self.node_version
        return self.node_version + sum((ancestry_versions[input_node] for input_node in self.input_nodes), Version())

    def check_and_load(self, op_store: "pipelines._OpStore", pipeline: "pipelines.Pipeline") -> "Node":
        """
        loads the op the node as persisted in node path
        raises ValueError if the node is not loadable

        :param op_store: the store to load the ops from
        :param pipeline: the pipeline to load this node for

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

    def persist(self, op_store: "pipelines._OpStore", pipeline_version: Version):
        """
        persists the inner op of the node in saver

        :param op_store: the store in which to store the node
        :param pipeline_version: the pipeline version of the op (including ancestry)
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
        res = self._op(*params)
        return res

    def check_and_load(self, op_store: "pipelines._OpStore", pipeline: "pipelines.Pipeline") -> "Node":
        """
        loads the op the node as persisted in node path
        raises ValueError if the node is not loadable

        :param pipeline: the pipeline to load the node for
        :type op_store: the op store to get the version from and collect bytes if needed
        :return: the loaded node
        """
        if not self.is_loadable:
            raise ValueError("trying to load a non loadable node")
        if isinstance(self._op, pipelines.Pipeline):
            self._op.load(op_store)
            return self
        op_version, saved_upstream_version = op_store.get_last_op_versions_from_pipeline(self._op, pipeline,
                                                                                         (None, None))
        if op_version is None:
            return self._load_any_version(op_store)
        self.check_version(op_version, saved_upstream_version, pipeline.get_pipeline_versions()[self])
        op_bytes = op_store.get_op_bytes_for_version(self._op, op_version)
        self._op.load(op_bytes)
        return self

    def check_version(self, persisted_op_version: Version, persisted_upstream_version: Version,
                      current_upstream_version: Version):
        """
        checks the current pipeline version (with ancestry) against the persisted version

        :param persisted_op_version: the inner version of the op at the time it was last persisted
        :param persisted_upstream_version: the compounded upstream version at the time of saving
        :param current_upstream_version: the compounded upstream version at time of check

        """
        print(persisted_op_version, persisted_upstream_version, current_upstream_version)
        if self.node_version.major != persisted_op_version.major:
            raise VersionError("cannot load an with different major version")
        if current_upstream_version.major != persisted_upstream_version.major:
            raise VersionError("cannot laod an op with different major upstream version")

    def _load_any_version(self, op_store: "pipelines._OpStore"):
        versions = op_store.get_all_verisons_of_op(self._op, None)
        if versions is None:
            return self
        op_bytes = op_store.get_op_bytes_for_version(self._op, max(versions))
        self._op.load(op_bytes)
        return self

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

    def persist(self, op_store: "pipelines._OpStore", pipeline: "pipelines.Pipeline"):
        """
        persists the inner op of the node in saver

        :param op_store: the store in which to store the node
        :param pipeline: the pipeline version of the op (including ancestry)
        """
        if not self.is_loadable:
            raise ValueError("trying to save a non savable/loadable op")
        if isinstance(self._op, pipelines.Pipeline):
            return self._op.save(op_store)
        op_store.save_op_bytes_for_pipeline(self._op, pipeline_name=pipeline.name,
                                            pipeline_version=pipeline.get_pipeline_versions()[self],
                                            op_bytes=self._op.serialize())

    @property
    def requires_runner(self) -> bool:
        """
        whether or not this node requires q runner to be executed
        (typically if the inner op is a pipelines)

        :return: bool
        """
        return isinstance(self._op, pipelines.Pipeline)

    def __repr__(self):
        return "<Node of {} with inputs {} and output {}>".format(self._op.name, self.input_nodes, self.output_node)


class DataNode(AbstractNode, metaclass=ABCMeta):

    def __init__(self, serializer: Serializer,  path: str, input_nodes: Optional[InputNodes] = None, output_node=None,
                 name: Optional[str] = None):
        """
        :param serializer: the serializer to use to load the dat
        :param path: the path to load the data from
        :param input_nodes: the input_nodes on which this node should be executed
        :param output_node: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
        :param name: the name of the op
        """

        super().__init__(input_nodes=input_nodes, output_node=output_node)
        self.path = path
        self.serializer = serializer
        self._name = name
        self._saver = None

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

    def __init__(self, serializer: Serializer,  path: str, output_node=None, name: Optional[str] = None):
        """
        :param serializer: the serializer to use to load the dat
        :param path: the path to load the data from
        :param output_node: an optional symbolic name for the node to be called by other node. If this node is the
        output of the pipeline use "pipeline_output" or `ReservedNodes.pipeline_output`
        :param name: the name of the op
        """
        super().__init__(serializer=serializer, path=path, output_node=output_node, name=name)

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
