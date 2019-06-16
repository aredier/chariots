from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Type, Mapping, Text, Union, Set, Tuple, Any, Dict

from chariots.core.ops import AbstractOp


SymbolicToRealMapping = Mapping[Text, Union["Node", "ReservedNodes"]]
ResultDict = Dict[Union["Node", "ReservedNodes"], Any]


class Node:

    def __init__(self, op: AbstractOp, input_nodes=None, output_node=None):
        self._op = op
        self.input_nodes = input_nodes or []
        self.output_node = output_node
        if self.output_node == ReservedNodes.pipeline_output:
            self.output_node = ReservedNodes.pipeline_output.value

    def replace_symbolic_references(self, symbolic_to_real_node: SymbolicToRealMapping) -> "Node":
        self.input_nodes = [self._ensure_node_is_real(node, symbolic_to_real_node) for node in self.input_nodes]
        return self

    @staticmethod
    def _ensure_node_is_real(node, symbolic_real_node_map: SymbolicToRealMapping):
        if isinstance(node, str):
            return symbolic_real_node_map[node]
        return node

    @property
    def has_symbolic_references(self):
        return any(isinstance(node, str) for node in self.input_nodes)

    def execute(self, *params):
        return self._op(*params)


class ReservedNodes(Enum):
    pipeline_input = "__pipeline_input__"
    pipeline_output = "__pipeline_output__"


class AbstractRunner(ABC):

    def __init__(self, nodes: List[Node]):
        self._graph = self.resolve_graph(nodes)

    @classmethod
    def resolve_graph(cls, nodes: List[Node]) -> List[Node]:
        symbolic_to_real_node_map = cls._build_symbolic_real_node_mapping(nodes)
        real_nodes = [node.replace_symbolic_references(symbolic_to_real_node_map) for node in nodes]
        cls._check_graph(real_nodes)
        return real_nodes

    @staticmethod
    def _build_symbolic_real_node_mapping(nodes: List[Node]) -> SymbolicToRealMapping:
        symbolic_to_real_mapping = {node.output_node: node for node in nodes if node.output_node}
        symbolic_to_real_mapping.update({node.value: node for node in ReservedNodes})
        return symbolic_to_real_mapping

    @classmethod
    def _check_graph(cls, nodes: List[Node]):
        available_nodes = {ReservedNodes.pipeline_input}
        for node in nodes:
            available_nodes = cls._update_ancestry(node, available_nodes)

    @classmethod
    def _update_ancestry(cls, node: Node, available_nodes: Set[Node]):
        orphan_nodes = [input_node for input_node in node.input_nodes if not input_node in available_nodes]
        if orphan_nodes:
            raise ValueError(f"cannot find node(s) {orphan_nodes} in ancestry")
        if node in available_nodes:
            raise ValueError("can only use a node node in a graph")
        return (available_nodes | {node}).difference(set(node.input_nodes))

    @abstractmethod
    def run_graph(self, pipeline_input):
        pass


class SequentialRunner(AbstractRunner):

    def run_graph(self, pipeline_input) -> ResultDict:
        temp_results = {ReservedNodes.pipeline_input: pipeline_input} if pipeline_input else {}
        for node in self._graph:
            temp_results = self._execute_node(node, temp_results)
        return temp_results

    @staticmethod
    def _execute_node(node: Node, temp_results: ResultDict) -> ResultDict:
        inputs = [temp_results.pop(input_node) for input_node in node.input_nodes]
        temp_results[node] = node.execute(*inputs)
        return temp_results


class Pipeline(AbstractOp):

    def __init__(self, nodes=List[Node], runner: Type[SequentialRunner] = SequentialRunner):
        self._runner = runner(nodes)

    def __call__(self, pipeline_input=None):
        results = self._runner.run_graph(pipeline_input)
        print(list(r.output_node for r in results))
        if len(results) > 1:
            raise ValueError("multiple pipeline outputs cases not handled")

        if results:
            return self.extract_results(results)

    @staticmethod
    def extract_results(results: Dict[Node, Any]):
        node, output = next(iter(results.items()))
        if node.output_node != ReservedNodes.pipeline_output.value:
            raise ValueError("received an output that is not a pipeline output")
        return output


