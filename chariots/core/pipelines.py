import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Mapping, Text, Union, Set, Any, Dict, Optional, AnyStr

from chariots.core.ops import AbstractOp, OPS_PATH, LoadableOp
from chariots.core.saving import Saver, JSONSerializer
from chariots.core.versioning import Version

SymbolicToRealMapping = Mapping[Text, Union["Node", "ReservedNodes"]]
ResultDict = Dict[Union["Node", "ReservedNodes"], Any]


PIPELINE_PATH = "/pipelines"


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
    def _ensure_node_is_real(node, symbolic_real_node_map: SymbolicToRealMapping) -> "Node":
        if isinstance(node, str):
            return symbolic_real_node_map[node]
        return node

    @property
    def node_version(self) -> Version:
        return self._op.__version__

    @property
    def has_symbolic_references(self) -> bool:
        return any(isinstance(node, str) for node in self.input_nodes)

    def execute(self, *params):
        res = self._op(*params)
        return res

    def get_version_with_ancestry(self, ancestry_versions):
        if not self.input_nodes:
            return self._op.__version__
        return self._op.__version__ + sum((ancestry_versions[input_node] for input_node in self.input_nodes), Version())

    def check_version(self, persisted_version: Mapping["Node", List[Version]],
                      current_versions: Mapping["Node", Version]):
        current_version = current_versions[self]
        last_loaded_version = max(persisted_version[self])
        if current_version > last_loaded_version and current_version.major != last_loaded_version.major:
            raise ValueError("trying to load incompatible version")

    def load(self, saver: Saver, node_path: Text) -> "Node":
        if not self.is_loadable:
            raise ValueError("trying to load a non loadable node")
        op_bytes = saver.load(node_path)
        self._op.load(op_bytes)
        return self

    @property
    def is_loadable(self) -> bool:
        return isinstance(self._op, LoadableOp)

    @property
    def name(self):
        return self._op.name

    def persist(self, saver: Saver, pipeline_version: Version):
        if not self.is_loadable:
            raise ValueError("trying to save a non savable/loadable op")
        op_bytes = self._op.serialize()
        saver.save(op_bytes, os.path.join(OPS_PATH, self.name, str(pipeline_version)))

    @property
    def requires_runner(self):
        return isinstance(self._op, Pipeline)

    def __repr__(self):
        return "<Node of {} with inputs {} and output {}>".format(self._op.name, self.input_nodes, self.output_node)


class ReservedNodes(Enum):
    pipeline_input = "__pipeline_input__"
    pipeline_output = "__pipeline_output__"


class AbstractRunner(ABC):

    @abstractmethod
    def run_graph(self, pipeline_input: Any, graph: List[Node]):
        pass


class SequentialRunner(AbstractRunner):

    def run_graph(self, pipeline_input: Any, graph: List[Node]) -> ResultDict:
        temp_results = {ReservedNodes.pipeline_input: pipeline_input} if pipeline_input else {}
        for node in graph:
            temp_results = self._execute_node(node, temp_results)
        return temp_results

    def _execute_node(self, node: Node, temp_results: ResultDict) -> ResultDict:
        inputs = [temp_results.pop(input_node) for input_node in node.input_nodes]
        if node.requires_runner:
            temp_results[node] = node.execute(self, *inputs)
            return temp_results
        temp_results[node] = node.execute(*inputs)
        return temp_results


class Pipeline(AbstractOp):

    def __init__(self, nodes: List[Node], name: Optional[AnyStr] = None):
        self._graph = self.resolve_graph(nodes)
        self._name = name

    @property
    def name(self):
        return self._name

    def set_pipeline_name(self, name: str):
        self._name = name

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
        orphan_nodes = [input_node for input_node in node.input_nodes if input_node not in available_nodes]
        if orphan_nodes:
            raise ValueError(f"cannot find node(s) {orphan_nodes} in ancestry")
        if node in available_nodes:
            raise ValueError("can only use a node node in a graph")
        update_available_node = available_nodes | {node}
        return update_available_node.difference(set(node.input_nodes))

    def __call__(self, runner: AbstractRunner, pipeline_input=None):
        results = runner.run_graph(pipeline_input=pipeline_input, graph=self._graph)
        if len(results) > 1:
            raise ValueError("multiple pipeline outputs cases not handled")

        if results:
            return self.extract_results(results)

    @staticmethod
    def extract_results(results: Dict[Node, Any]) -> Any:
        node, output = next(iter(results.items()))
        if node.output_node != ReservedNodes.pipeline_output.value:
            raise ValueError("received an output that is not a pipeline output")
        return output

    def get_pipeline_versions(self) -> Mapping[Node, Version]:
        versions = {}
        for node in self._graph:
            versions[node] = node.get_version_with_ancestry(versions)
        return versions

    def load(self, saver: Saver):
        persisted_versions = self._load_versions(saver)

        new_graph = self._graph
        for i, node in enumerate(self._graph):
            new_node = self._load_single_node(node, persisted_versions, saver)
            new_graph[i] = new_node
            self._graph = new_graph
        return self

    def _load_single_node(self, node: Node, versions: Mapping[Node, List[Version]], saver: Saver):
        if not node.is_loadable:
            return node
        node.check_version(persisted_version=versions, current_versions=self.get_pipeline_versions())
        node_path = self._get_path_from_versions(versions, node)
        return node.load(saver, node_path)

    def _load_versions(self, saver: Saver) -> Mapping[Node, List[Version]]:
        if self.name is None:
            raise ValueError("pipeline has no name, cannot load")
        try:
            pipeline_bytes = saver.load(self.pipeline_meta_path)
            pipeline_json = JSONSerializer().deserialize_object(pipeline_bytes)
        except FileNotFoundError:
            pipeline_json = {}
        return {
            self.node_for_name[node_name]: [Version.parse(version_str) for version_str in versions]
            for node_name, versions in pipeline_json.items()
        }

    @property
    def pipeline_meta_path(self):
        return os.path.join(PIPELINE_PATH, self.name, "_meta.json")

    @staticmethod
    def _get_path_from_versions(versions: Mapping[Node, List[Version]], node: Node) -> Text:
        node_version = max(versions[node])
        return os.path.join(OPS_PATH, node.name, str(node_version))

    @property
    def node_for_name(self):
        return {node.name: node for node in self._graph}

    def save(self, saver: Saver):
        persisted_versions = self._load_versions(saver)
        pipeline_versions = self.get_pipeline_versions()
        if not persisted_versions:
            self._save_meta({node: [node_version] for node, node_version in pipeline_versions.items()}, saver)
            return self._persist_nodes(saver, pipeline_versions)
        for node in self._graph:
            persisted_versions = self._update_versions(persisted_versions, pipeline_versions, node)
        self._save_meta(persisted_versions, saver)
        return self._persist_nodes(saver, pipeline_versions)

    def _save_meta(self, meta: Mapping[Node, List[Version]], saver: Saver):
        new_meta_bytes = JSONSerializer().serialize_object({
            node.name: [str(version) for version in node_versions]
            for node, node_versions in meta.items()
        })
        saver.save(new_meta_bytes, self.pipeline_meta_path)

    def _persist_nodes(self, saver: Saver, pipeline_versions: Mapping[Node, Version]):
        for node in self._graph:
            if node.is_loadable:
                node.persist(saver, pipeline_versions[node])

    @staticmethod
    def _update_versions(historic_versions: Mapping[Node, List[Version]],
                         pipeline_versions: Mapping[Node, Version], node: Node):
        if not node.is_loadable:
            return historic_versions
        if pipeline_versions[node] in historic_versions[node]:
            return historic_versions
        historic_versions[node].append(pipeline_versions[node])
        return historic_versions
