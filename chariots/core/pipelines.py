import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Mapping, Text, Set, Any, Dict, Optional

from chariots.core import nodes
from chariots.core.op_store import OpStore
from chariots.core.ops import AbstractOp
from chariots.core.saving import Saver
from chariots.core.versioning import Version
from chariots.helpers.typing import ResultDict, SymbolicToRealMapping

PIPELINE_PATH = "/pipelines"


class ReservedNodes(Enum):
    """
    enum of reserved node names
    """

    pipeline_input = "__pipeline_input__"
    pipeline_output = "__pipeline_output__"

    @property
    def reference(self):
        return nodes.NodeReference(self, self.value)


class AbstractRunner(ABC):
    """
    a runner handles executing a graph of nodes
    """

    @abstractmethod
    def run_graph(self, pipeline_input: Any, graph: List["nodes.AbstractNode"]) -> Optional[Any]:
        """
        executes the whole graph of q pipeline

        :param pipeline_input: the input to be given to the pipeline
        :param graph: the list of nodes
        :return: the output of the graph called on the input if applicable
        """
        pass


class SequentialRunner(AbstractRunner):
    """
    runner that executes a node graph sequentially
    """

    def run_graph(self, pipeline_input: Any, graph: List["nodes.AbstractNode"]) -> ResultDict:
        temp_results = {ReservedNodes.pipeline_input.reference: pipeline_input} if pipeline_input else {}
        for node in graph:
            temp_results = self._execute_node(node, temp_results)
        return temp_results

    def _execute_node(self, node: "nodes.AbstractNode", temp_results: ResultDict) -> ResultDict:
        inputs = [temp_results.pop(input_node) for input_node in node.input_nodes]
        if node.requires_runner:
            res = node.execute(self, *inputs)
        else:
            res = node.execute(*inputs)
        if not isinstance(res, tuple):
            res = (res,)
        if len(res) != len(node.output_references):
            raise ValueError("found output with inconsistent size for {} got {} and "
                             "expected".format(node.name, len(res), len(node.output_references)))
        temp_results.update(dict(zip(node.output_references, res)))
        return temp_results


class Pipeline(AbstractOp):
    """
    a pipeline is a collection of linked nodes to be executed together
    """

    def __init__(self, pipeline_nodes: List["nodes.AbstractNode"], name: str):
        """
        :param pipeline_nodes: the nodes of the pipeline
        :param name: the name of the pipeline
        """
        self._graph = self.resolve_graph(pipeline_nodes)
        self._name = name

    def prepare(self, saver: Saver):
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

    @classmethod
    def resolve_graph(cls, pipeline_nodes: List["nodes.AbstractNode"]) -> List["nodes.AbstractNode"]:
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
    def _build_symbolic_real_node_mapping(pipeline_nodes: List["nodes.AbstractNode"]) -> SymbolicToRealMapping:
        """
        builds a mapping of nodes with their symbolic name in key and the object in value

        :param pipeline_nodes: the nodes to build the mapping from
        :return: the mapping
        """
        symbolic_to_real_mapping = {output_ref.reference: output_ref
                                    for node in pipeline_nodes if node.output_nodes
                                    for output_ref in node.output_references
                                    }
        symbolic_to_real_mapping.update({node.value: node.reference for node in ReservedNodes})
        return symbolic_to_real_mapping

    @classmethod
    def _check_graph(cls, pipeline_nodes: List["nodes.AbstractNode"]):
        """
        checks a graph for potential problems.
        raises if a node's input is not in the graph or if a node is used twice in the pipeline

        :param pipeline_nodes: the nodes to check
        """
        available_nodes = {ReservedNodes.pipeline_input.reference}
        for node in pipeline_nodes:
            available_nodes = cls._update_ancestry(node, available_nodes)

    @classmethod
    def _update_ancestry(cls, node: "nodes.AbstractNode",
                         available_nodes: Set["nodes.NodeReference"]) -> Set["nodes.NodeReference"]:
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

    def __call__(self, runner: AbstractRunner, pipeline_input=None):
        results = runner.run_graph(pipeline_input=pipeline_input, graph=self._graph)
        if len(results) > 1:
            raise ValueError("multiple pipeline outputs cases not handled, got {}".format(results))

        if results:
            return self.extract_results(results)

    @staticmethod
    def extract_results(results: Dict["nodes.NodeReference", Any]) -> Any:
        """
        extracts the output of a pipeline.
        raises ValueError if some output was unused once every node is computed and the remaining is not the output of
        the pipeline

        :param results: the outputs left unused once the graph has ran
        :return: the result
        """
        node_reference, output = next(iter(results.items()))
        if output is not None and node_reference.reference != ReservedNodes.pipeline_output.value:
            raise ValueError("received an output that is not a pipeline output")
        return output

    def get_pipeline_versions(self) -> Mapping["nodes.AbstractNode", Version]:
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
    def _check_and_load_single_node(op_store: OpStore, upstream_node: "nodes.AbstractNode",
                                    downstream_node: Optional["nodes.AbstractNode"]) -> "nodes.AbstractNode":
        latest_node = upstream_node.load_latest_version(op_store)
        if latest_node is None:
            upstream_node.persist(op_store, [downstream_node] if downstream_node else None)
            return upstream_node

        if downstream_node is None:
            return latest_node
        downstream_node.check_version_compatibility(latest_node, op_store)
        return latest_node

    @property
    def pipeline_meta_path(self) -> str:
        """
        generates the path of the meta of this op

        :return: the string of the path
        """
        return os.path.join(PIPELINE_PATH, self.name, "_meta.json")

    @property
    def node_for_name(self) -> Mapping[Text, "nodes.AbstractNode"]:
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

    def _find_downstream(self, upstream_node: "nodes.AbstractNode") -> Optional["nodes.AbstractNode"]:
        """
        finds the downstream node from an upstream if it exists

        :param upstream_node: the upstream node to find the downstream of
        """
        return next((node for node in self._graph if upstream_node in [ref.node for ref in node.input_nodes]), None)
