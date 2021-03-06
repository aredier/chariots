"""module for the `Pipeline` class"""
from typing import List, Optional, Set, Dict, Any, Mapping, Text, Tuple

from .. import op_store
from ..versioning import Version
from . import nodes, callbacks, ops, runners  # pylint: disable=unused-import; # noqa
from .nodes._base_nodes import NodeReference
from .._helpers.typing import SymbolicToRealMapping, ResultDict


class Pipeline(ops.BaseOp):
    """
    a pipeline is a collection of linked nodes that have to be executed one on top of each other. pipelines are the main
    way to use Chariots.

    to build a simple pipeline you can do as such:

    .. testsetup::

        >>> import tempfile
        >>> import shutil

        >>> from chariots.pipelines import Pipeline
        >>> from chariots.pipelines.nodes import Node
        >>> from chariots.testing import TestOpStoreClient
        >>> from chariots._helpers.doc_utils import AddOneOp, IsOddOp
        >>> app_path = tempfile.mkdtemp()
        >>> op_store_client = TestOpStoreClient(app_path)
        >>> op_store_client.server.db.create_all()

    .. doctest::

        >>> pipeline = Pipeline([
        ...     Node(AddOneOp(), input_nodes=["__pipeline_input__"], output_nodes=["added_number"]),
        ...     Node(IsOddOp(), input_nodes=["added_number"], output_nodes=["__pipeline_output__"])
        ... ], "simple_pipeline")

    here we have just created a very simple pipeline with two nodes, one that adds one to the provided number and one
    that returns whether or not the resulting number is odd

    to use our pipeline, we can either do it manually with a runner:

    .. doctest::

        >>> from chariots.pipelines.runners import SequentialRunner
        >>> runner = SequentialRunner()
        >>> runner.run(pipeline=pipeline, pipeline_input=4)
        True

    you can also as easily deploy your pipeline to a Chariots app (small micro-service to run your pipeline)

    .. doctest::

        >>> from chariots.pipelines import PipelinesServer
        >>> app = PipelinesServer([pipeline], op_store_client=op_store_client, import_name="simple_app")

    Once this is done you can deploy your app as a flask app and get the result of the pipeline using a client:

    .. testsetup::

        >>> from chariots.testing import TestPipelinesClient
        >>> client = TestPipelinesClient(app)

    .. doctest::

        >>> client.call_pipeline(pipeline, 4).value
        True

    .. testsetup::
        >>> shutil.rmtree(app_path)

    :param pipeline_nodes: the nodes of the pipeline. each node has to be linked to previous node
                           (or `__pipeline_input__`). nodes can create branches but the only output remaining has to be
                           `__pipeline_output__` (or no ouptut)
    :param name: the name of the pipeline. this will be used to create the route at which to query the pipeline in the
                 Chariots app
    :param pipeline_callbacks: callbacks to be used with this pipeline (monitoring and logging for instance)
    :param use_worker: whether or not to execute this pipeline in a separate worker (rather than in the main server) by
                       default. If set to `False`, the setting can still be overridden on an execution per execution
                       basis using the client.
    """

    def __init__(self, pipeline_nodes: List['nodes.BaseNode'], name: str,
                 pipeline_callbacks: Optional[List[callbacks.PipelineCallback]] = None,
                 use_worker: Optional[bool] = None):
        """
        """
        super().__init__(pipeline_callbacks)
        self._graph = self._resolve_graph(pipeline_nodes)
        self._name = name
        self.use_worker = use_worker

    @property
    def name(self) -> str:
        """the name of the pipeline"""
        return self._name

    @property
    def pipeline_nodes(self):
        """the nodes of the pipeline"""
        return self._graph

    @classmethod
    def _resolve_graph(cls, pipeline_nodes: List['nodes.BaseNode']) -> List['nodes.BaseNode']:
        """
        transforms a user provided graph into a usable graph: checking linkage, replacing symbolic references by
        real ones, ...

        :param pipeline_nodes: the list of linked nodes
        :return: the transformed list of linked nodes
        """
        symbolic_to_real_node_map = cls._build_symbolic_real_node_mapping(pipeline_nodes)
        real_nodes = [node.replace_symbolic_references(symbolic_to_real_node_map) for node in pipeline_nodes]
        cls._check_graph(real_nodes)
        return real_nodes

    @staticmethod
    def _build_symbolic_real_node_mapping(pipeline_nodes: List[nodes.BaseNode]) -> SymbolicToRealMapping:
        """
        builds a mapping of nodes with their symbolic name in key and the object in value

        :param pipeline_nodes: the nodes to build the mapping from
        :return: the mapping
        """
        symbolic_to_real_mapping = {output_ref.reference: output_ref
                                    for node in pipeline_nodes if node.output_nodes
                                    for output_ref in node.output_references
                                    }
        symbolic_to_real_mapping.update({node.value: node.reference for node in nodes.ReservedNodes})
        return symbolic_to_real_mapping

    @classmethod
    def _check_graph(cls, pipeline_nodes: List['nodes.BaseNode']):
        """
        checks a graph for potential problems.
        raises if a node's input is not in the graph or if a node is used twice in the pipeline

        :param pipeline_nodes: the nodes to check
        """
        available_nodes = {nodes.ReservedNodes.pipeline_input.reference}
        for node in pipeline_nodes:
            available_nodes = cls._update_ancestry(node, available_nodes)

    @classmethod
    def _update_ancestry(cls, node: 'nodes.BaseNode',
                         available_nodes: Set[NodeReference]) -> Set[NodeReference]:
        """
        updates the list of available nodes with a node of interest if possible

        :param node: the node of interest
        :param available_nodes: the available nodes to date
        :return:  the updated ancestry
        """
        orphan_nodes = [input_node for input_node in node.input_nodes if input_node not in available_nodes]
        if orphan_nodes:
            raise ValueError('cannot find node(s) {} in ancestry'.format(orphan_nodes))
        if node in available_nodes:
            raise ValueError('can only use a node in a graph')
        update_available_node = available_nodes | set(node.output_references)
        return set(node_ref for node_ref in update_available_node if node_ref not in node.input_nodes)

    def execute(self, runner: 'base.BaseRunner', pipeline_input=None):  # pylint: disable=arguments-differ
        """present for inheritance purposes from the Op Class, this will automatically raise"""
        raise ValueError('pipelines cannot be executed through the `execute`method. use a runner with '
                         '`runner.run(this_pipeline)`')

    @staticmethod
    def extract_results(results: Dict[NodeReference, Any]) -> Any:
        """
        extracts the output of a pipeline once all the nodes have been computed.
        This method is used by runners when once all the nodes are computed in order to check and get the final result
        to return

        :param results: the outputs left unused once the graph has been ran.

        :raises ValueError: if some output was unused once every node is computed and the remaining is not the output of
                            the pipeline
        :return: the final result of the pipeline as needs to be returned to the use
        """
        node_reference, output = next(iter(results.items()))
        if output is not None and node_reference.reference != nodes.ReservedNodes.pipeline_output.value:
            raise ValueError('received an output that is not a pipeline output')
        return output

    def execute_node(self, node: nodes.BaseNode, intermediate_results: ResultDict, runner: 'runners.BaseRunner'):
        """
        executes a node from the pipeline, this method is called by the runners to make the pipeline execute one of it's
        node and all necessary callbacks

        :param node: the node to be executed
        :param intermediate_results: the intermediate result to look in in order to fin the node's inputs
        :param runner: a runner to be used in case the node needs a runner to be executed (internal pipeline)

        :raises ValueError: if the output of the node does not correspond to the length of it's output references

        :return: the final result of the node after the execution
        """
        inputs = [intermediate_results.pop(input_node) for input_node in node.input_nodes]

        for callback in self.callbacks:
            callback.before_node_execution(self, node, inputs)

        # we are providing a runner just in case one is needed
        res = node.execute(inputs, runner)

        for callback in self.callbacks:
            callback.after_node_execution(self, node, inputs, res)

        # recasting result to have consistent output
        if not isinstance(res, tuple):
            res = (res,)

        if len(res) != len(node.output_references):
            raise ValueError('found output with inconsistent size for {} got {} and '
                             'expected {}'.format(node.name, len(res), len(node.output_references)))

        intermediate_results.update(dict(zip(node.output_references, res)))
        return intermediate_results

    def get_pipeline_versions(self) -> Mapping['nodes.BaseNode', Version]:
        """
        returns the versions of every op in the pipeline

        :return: the mapping version for node
        """
        return {node: node.node_version for node in self._graph}

    def load(self, op_store_client: op_store.OpStoreClient):
        """
        loads all the latest versions of the nodes in the pipeline if they are compatible from an `OpStore`. if the
        latest version is not compatible, it will raise a `VersionError`

        :param op_store_client: the op store to look for existing versions if any and to load the bytes of said version
        if possible

        :raises VersionError: if a node is incompatible with one of it's input. For instance if a node has not been
                              trained on the latest version of it's input in an inference pipeline

        :return: this pipeline once it has been fully loaded
        """
        for i in range(len(self._graph)):
            # we are checking the nodes (from upstream down) and provide the node we are checking
            # against the one next node (that it needs to be compatible with)
            upstream_node = self._graph[i]
            downstream_node = self._find_downstream(upstream_node)
            self._graph[i] = self._check_and_load_single_node(op_store_client, upstream_node, downstream_node)
        return self

    @staticmethod
    def _check_and_load_single_node(op_store_client: op_store.OpStoreClient, upstream_node: 'nodes.BaseNode',
                                    downstream_node: Optional['nodes.BaseNode']) -> 'nodes.BaseNode':
        latest_node = upstream_node.load_latest_version(op_store_client)

        if downstream_node is not None:
            downstream_node.check_version_compatibility(latest_node, op_store_client)
        return latest_node

    @property
    def node_for_name(self) -> Mapping[Text, 'nodes.BaseNode']:
        """utils mapping that has node names in input and the nodes objects in values"""
        return {node.name: node for node in self._graph}

    def save(self, op_store_client: op_store.OpStoreClient):
        """
        persists all the nodes (that need saving) in an `OpStore`. this is used for instance when a training pipeline
        has been executed and needs to save it's trained node(s) for the inference pipeline to load them. This method
        also updates the versions available for the store to serve in the future

        :param op_store_client: the store to persist the nodes and their versions in
        """

        for upstream_node, downstream_node in self.get_all_op_links():
            upstream_node.persist(op_store_client, [downstream_node] if downstream_node else None)

    def _find_downstream(self, upstream_node: 'nodes.BaseNode') -> Optional['nodes.BaseNode']:
        """
        finds the downstream node from an upstream if it exists

        :param upstream_node: the upstream node to find the downstream of
        """
        return next((node for node in self._graph if upstream_node in [ref.node for ref in node.input_nodes]), None)

    def get_all_op_links(self) -> List[Tuple['nodes.BaseNode', 'nodes.BaseNode']]:
        """gets all the links present in the pipeline"""
        return [(upstream_node, self._find_downstream(upstream_node)) for upstream_node in self._graph]
