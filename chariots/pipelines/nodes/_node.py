"""module for the most basic node class"""
from typing import Optional, List, Union, Text, Any

# use the main package to resolve circular imports with root objects (Pipleine, ...)
from ... import versioning, op_store, pipelines
from .. import ops, runners
from . import BaseNode


class Node(BaseNode):
    """
    Class that handles the interaction between a pipeline and an Op. it handles defining the nodes that are going to be
    used as the inputs of the op and how the output of the op should be reppresented for the rest of the pipeline.


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

    :param op: the op this Node represents
    :param input_nodes: the input_nodes that are going to be used as inputs of the inner op the node, the inputs will
                        be given to the op in the order they are defined in this argument.
    :param output_nodes: a symbolic name for the the output(s) of the op, if the op returns a tuple `output_noes`
                         should be the same length as said tuple
    """

    def __init__(self, op: ops.BaseOp, input_nodes: Optional[List[Union[Text, BaseNode]]] = None,
                 output_nodes: Union[List[Union[Text, BaseNode]], Text, BaseNode] = None):
        self._op = op
        super().__init__(input_nodes=input_nodes, output_nodes=output_nodes)

    @property
    def node_version(self) -> versioning.Version:
        return self._op.op_version

    def execute(self, params: List[Any],  # pylint: disable=arguments-differ
                runner: Optional[runners.BaseRunner] = None) -> Any:
        """
        executes the underlying op on params

        :param runner: runner that can be provided if the node needs one (mostly if node is a pipeline)
        :param params: the inputs of the underlying op

        :raises ValueError: if the runner is not provided but needed

        :return: the output of the op
        """
        if self.requires_runner and runner is None:
            raise ValueError('runner was not provided but is required to execute this node')
        if self.requires_runner:
            # if no params, the pipeline should be executed with None
            return runner.run(self._op, params if params else None)
        return self._op.execute_with_all_callbacks(params)

    def load_latest_version(self, store_to_look_in: 'op_store.OpStoreClient') -> Optional[BaseNode]:
        """
        reloads the latest version of the op this node represents by looking for available versions in the store

        :param store_to_look_in:  the store to look for new versions in

        :return: the reloaded node if any older versions where found in the store otherwise `None`
        """
        if not self.is_loadable:
            return self
        if isinstance(self._op, pipelines.Pipeline):
            self._op.load(store_to_look_in)
            return self
        all_versions = store_to_look_in.get_all_versions_of_op(self._op)
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

    def check_version_compatibility(self, upstream_node: 'BaseNode', store_to_look_in: 'op_store.OpStoreClient'):
        if self._op.allow_version_change:
            return
        super().check_version_compatibility(upstream_node, store_to_look_in)

    @property
    def is_loadable(self) -> bool:
        """
        :return: whether or not this node and its inner op can be loaded
        """
        return isinstance(self._op, (ops.LoadableOp, pipelines.Pipeline))

    @property
    def name(self) -> str:
        """the name of the node. by default this will be the name of the underlying op."""
        return self._op.name

    def persist(self, store: 'op_store.OpStoreClient',
                downstream_nodes: Optional[List['BaseNode']]) -> Optional[versioning.Version]:

        version = super().persist(store, downstream_nodes)
        if not self.is_loadable:
            return None

        # protected access needed to solve circular imports
        if isinstance(self._op, pipelines.Pipeline):  # pylint: disable=protected-access
            return self._op.save(store)
        store.save_op_bytes(self._op, version, op_bytes=self._op.serialize())
        return version

    @property
    def requires_runner(self) -> bool:
        # protected access needed to solve circular imports
        return isinstance(self._op, pipelines.Pipeline)  # pylint: disable=protected-access

    def __repr__(self):
        return '<Node of {} with inputs {} and output {}>'.format(self._op.name, self.input_nodes, self.output_nodes)
