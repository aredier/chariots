from typing import List, Optional, Set, Dict, Any, Mapping, Text

from chariots import base
from chariots import nodes
from chariots import callbacks
from chariots.versioning import Version
from ._helpers.typing import SymbolicToRealMapping, ResultDict
from ._op_store import OpStore
from chariots.base._base_nodes import NodeReference


class Pipeline(base.BaseOp):
    """
    a pipeline is a collection of linked nodes to be executed together
    """

    def __init__(self, pipeline_nodes: List["base.BaseNode"], name: str,
                 pipeline_callbacks: Optional[List["callbacks.PipelineCallback"]] = None):
        """
        :param pipeline_nodes: the nodes of the pipeline
        :param name: the name of the pipeline
        :param pipeline_callbacks: the pipeline callbacks to use with this pipeline
        """
        super().__init__(pipeline_callbacks)
        self._graph = self.resolve_graph(pipeline_nodes)
        self._name = name

    def prepare(self, saver: "base.BaseSaver"):
        for node in self._graph:
            if node.require_saver:
                node.attach_saver(saver)

    @property
    def name(self) -> str:
        """
        the name of the pipeline
        :return: string of the name
        """
        return self._name

    @property
    def nodes(self):
        """the nodes of the pipeline"""
        return self._graph

    @classmethod
    def resolve_graph(cls, pipeline_nodes: List["base.BaseNode"]) -> List["base.BaseNode"]:
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
    def _build_symbolic_real_node_mapping(pipeline_nodes: List["base.BaseNode"]) -> SymbolicToRealMapping:
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
    def _check_graph(cls, pipeline_nodes: List["base.BaseNode"]):
        """
        checks a graph for potential problems.
        raises if a node's input is not in the graph or if a node is used twice in the pipeline

        :param pipeline_nodes: the nodes to check
        """
        available_nodes = {nodes.ReservedNodes.pipeline_input.reference}
        for node in pipeline_nodes:
            available_nodes = cls._update_ancestry(node, available_nodes)

    @classmethod
    def _update_ancestry(cls, node: "base.BaseNode",
                         available_nodes: Set["NodeReference"]) -> Set["NodeReference"]:
        """
        updates the list of available nodes with a node of interest if possible

        :param node: the node of interest
        :param available_nodes: the available nodes to date
        :return:  the updated ancestry
        """
        orphan_nodes = [input_node for input_node in node.input_nodes if input_node not in available_nodes]
        if orphan_nodes:
            raise ValueError("cannot find node(s) {} in ancestry".format(orphan_nodes))
        if node in available_nodes:
            raise ValueError("can only use a node in a graph")
        update_available_node = available_nodes | set(node.output_references)
        return set(node_ref for node_ref in update_available_node if node_ref not in node.input_nodes)

    def execute(self, runner: "base.BaseRunner", pipeline_input=None):
        raise ValueError("pipelines cannot be executed through the `execute`method. use a runner with "
                         "`runner.run(this_pipeline)`")

    @staticmethod
    def extract_results(results: Dict["NodeReference", Any]) -> Any:
        """
        extracts the output of a pipeline.
        raises ValueError if some output was unused once every node is computed and the remaining is not the output of
        the pipeline

        :param results: the outputs left unused once the graph has ran
        :return: the result
        """
        node_reference, output = next(iter(results.items()))
        if output is not None and node_reference.reference != nodes.ReservedNodes.pipeline_output.value:
            raise ValueError("received an output that is not a pipeline output")
        return output

    def execute_node(self, node: "base.BaseNode", intermediate_results: ResultDict, runner: "base.BaseRunner"):
        """
        executes a node for the pipeline, this method is called by the runners to make the pipeline execute one of it's
        node and all necessary callbacks

        :param node: the node to execute
        :param intermediate_results: the intermediate result to look in in order to fin the node's inputs
        :param runner: a runner to be used in case the node needs a runner to be executed (internal pipeline)

        :raises ValueError: if the output of the node does not correspond to the length of it's output reference

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
            raise ValueError("found output with inconsistent size for {} got {} and "
                             "expected".format(node.name, len(res), len(node.output_references)))

        intermediate_results.update(dict(zip(node.output_references, res)))
        return intermediate_results

    def get_pipeline_versions(self) -> Mapping["base.BaseNode", Version]:
        """
        returns the versions of every op in the pipeline

        :return: the mapping version for node
        """
        return {node: node.node_version for node in self._graph}

    def load(self, op_store: OpStore):
        """
        loads this pipeline as last saved in saver

        :type op_store: the op store to collect the ops and versions from
        :return: this pipeline loaded
        """
        for i in range(len(self._graph)):
            # we are checking the nodes (from upstream down) and provide the node we are checking
            # against the one next node (that it needs to be compatible with)
            upstream_node = self._graph[i]
            downstream_node = self._find_downstream(upstream_node)
            self._graph[i] = self._check_and_load_single_node(op_store, upstream_node, downstream_node)
        return self

    @staticmethod
    def _check_and_load_single_node(op_store: OpStore, upstream_node: "base.BaseNode",
                                    downstream_node: Optional["base.BaseNode"]) -> "base.BaseNode":
        latest_node = upstream_node.load_latest_version(op_store)
        if latest_node is None:
            upstream_node.persist(op_store, [downstream_node] if downstream_node else None)
            return upstream_node

        if downstream_node is None:
            return latest_node
        downstream_node.check_version_compatibility(latest_node, op_store)
        return latest_node

    @property
    def node_for_name(self) -> Mapping[Text, "base.BaseNode"]:
        """
        generates a mapping with each nodes's name in key and the object as value

        :return: the mapping
        """
        return {node.name: node for node in self._graph}

    def save(self, op_store: OpStore):
        """
        saves this pipeline in saver

        :param op_store: the store to persist ops in
        """
        for i in range(len(self._graph)):
            upstream_node = self._graph[i]
            downstream_node = self._find_downstream(upstream_node)
            upstream_node.persist(op_store, [downstream_node] if downstream_node else None)

    def _find_downstream(self, upstream_node: "base.BaseNode") -> Optional["base.BaseNode"]:
        """
        finds the downstream node from an upstream if it exists

        :param upstream_node: the upstream node to find the downstream of
        """
        return next((node for node in self._graph if upstream_node in [ref.node for ref in node.input_nodes]), None)
