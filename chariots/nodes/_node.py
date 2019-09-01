from typing import Optional, List, Union, Text, Any

# use the main package to resolve circular imports with root objects (Pipleine, ...)
import chariots
from chariots.base import BaseNode
from chariots.base import BaseOp
from chariots.ops import LoadableOp
from chariots.versioning import Version


class Node(BaseNode):
    """
    a Node handles the interaction of an op with other ops/nodes.
    It represents a slot in the pipeline.
    """

    def __init__(self, op: BaseOp, input_nodes: Optional[List[Union[Text, "BaseNode"]]] = None,
                 output_nodes: Union[List[Union[Text, "BaseNode"]], Text, "BaseNode"] = None):
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

    def execute(self, params: List[Any], runner: Optional["_pipelines.AbstractRunner"] = None) -> Any:
        """
        executes the underlying op on params

        :param runner: runner that can be provided if the node needs one
        :param params: the inputs of the underlying op

        :raises ValueError: if the runner is not provided but needed

        :return: the output of the op
        """
        if self.requires_runner and runner is None:
            raise ValueError("runner was not provided but is required to execute this node")
        if self.requires_runner:
            return runner.run(self._op, params)
        return self._op.execute_with_all_callbacks(params)

    def load_latest_version(self, store_to_look_in: "chariots.OpStore") -> "BaseNode":
        """
        reloads the latest version of this op by looking into the available versions of the store
        :param store_to_look_in:  the store to look for new versions in
        :return:
        """
        if not self.is_loadable:
            return self
        if isinstance(self._op, chariots.Pipeline):
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

    def check_version_compatibility(self, upstream_node: "BaseNode", store_to_look_in: "_op_store.OpStore"):
        if self._op.allow_version_change:
            return
        super().check_version_compatibility(upstream_node, store_to_look_in)

    @property
    def is_loadable(self) -> bool:
        """
        :return: whether or not this node and its inner op can be loaded
        """
        return isinstance(self._op, (LoadableOp, chariots.Pipeline))

    @property
    def name(self) -> str:
        """
        the name of the node

        :return: the string of the name
        """
        return self._op.name

    def persist(self, store: "chariots.OpStore", downstream_nodes: Optional[List["BaseNode"]]) -> Optional[Version]:
        """
        persists the inner op of the node in saver

        :param store: the store in which to store the node
        :param downstream_nodes: the node(s) that are going to accept this node downstrem
        """

        version = super().persist(store, downstream_nodes)
        if not self.is_loadable:
            return
        if isinstance(self._op, chariots._pipeline.Pipeline):
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
        return isinstance(self._op, chariots._pipeline.Pipeline)

    def __repr__(self):
        return "<Node of {} with inputs {} and output {}>".format(self._op.name, self.input_nodes, self.output_nodes)