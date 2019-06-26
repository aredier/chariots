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
        self.input_nodes = input_nodes or []
        self.output_node = output_node
        if self.output_node == ReservedNodes.pipeline_output:
            self.output_node = ReservedNodes.pipeline_output.value

    def replace_symbolic_references(self, symbolic_to_real_node: SymbolicToRealMapping) -> "Node":
        """
        replaces symbolic references (input_nodes specified as strings) by the objects they reference

        :param symbolic_to_real_node: the mapping of nodes for their symbolic name
        :return: this node with it's symbolic inputs replaced
        """
        self.input_nodes = [self._ensure_node_is_real(node, symbolic_to_real_node) for node in self.input_nodes]
        return self

    @staticmethod
    def _ensure_node_is_real(node, symbolic_real_node_map: SymbolicToRealMapping) -> "Node":
        if isinstance(node, str):
            return symbolic_real_node_map[node]
        return node

    @property
    def node_version(self) -> Version:
        """
        the version of the op this node represents
        """
        return self._op.__version__

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

    def get_version_with_ancestry(self, ancestry_versions: Mapping["Node", Version]) -> Version:
        """
        adds this node's version to those of it's input (themselves computed on their ancestry)

        :param ancestry_versions: a mapping with a version for each op which's version with ancestry has been computed
        :return: the resulting version
        """
        if not self.input_nodes:
            return self._op.__version__
        return self._op.__version__ + sum((ancestry_versions[input_node] for input_node in self.input_nodes), Version())

    def check_version(self, persisted_versions: Mapping["Node", List[Version]],
                      current_versions: Mapping["Node", Version]):
        """
        checks the current pipeline version (with ancestry) against the persisted version

        :param persisted_versions: the persisted versions of loadable ops in the pipeline
        :param current_versions: the current versions of the ops (as submitted in the pipeline)
        """
        current_version = current_versions[self]
        last_loaded_version = max(persisted_versions[self])
        if current_version > last_loaded_version and current_version.major != last_loaded_version.major:
            raise ValueError("trying to load incompatible version")

    def load(self, saver: Saver, node_path: Text) -> "Node":
        """
        loads the op the node as persisted in node path
        raises ValueError if the node is not loadable

        :param saver: the saver to load the op from
        :param node_path: the path the node as persisted at
        :return: the loaded node
        """
        if not self.is_loadable:
            raise ValueError("trying to load a non loadable node")
        if isinstance(self._op, Pipeline):
            self._op.load(saver)
            return self
        op_bytes = saver.load(node_path)
        self._op.load(op_bytes)
        return self

    @property
    def is_loadable(self) -> bool:
        """
        :return: whether or not this node and its inner op can be loaded
        """
        return isinstance(self._op, (LoadableOp, Pipeline))

    @property
    def name(self) -> str:
        """
        the name of the node

        :return: the string of the name
        """
        return self._op.name

    def persist(self, saver: Saver, pipeline_version: Version):
        """
        persists the inner op of the node in saver

        :param pipeline_version: the pipeline version of the op (including ancestry)
        :param saver: the saver to save the op in
        """
        if not self.is_loadable:
            raise ValueError("trying to save a non savable/loadable op")
        if isinstance(self._op, Pipeline):
            return self._op.save(saver)
        op_bytes = self._op.serialize()
        saver.save(op_bytes, os.path.join(OPS_PATH, self.name, str(pipeline_version)))

    @property
    def requires_runner(self) -> bool:
        """
        whether or not this node requires q runner to be executed
        (typically if the inner op is a pipelines)

        :return: bool
        """
        return isinstance(self._op, Pipeline)

    def __repr__(self):
        return "<Node of {} with inputs {} and output {}>".format(self._op.name, self.input_nodes, self.output_node)


class ReservedNodes(Enum):
    """
    enum of reserved node names
    """

    pipeline_input = "__pipeline_input__"
    pipeline_output = "__pipeline_output__"


class AbstractRunner(ABC):
    """
    a runner handles executing a graph of nodes
    """

    @abstractmethod
    def run_graph(self, pipeline_input: Any, graph: List[Node]) -> Optional[Any]:
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
    """
    a pipeline is a collection of linked nodes to be executed together
    """

    def __init__(self, nodes: List[Node], name: Optional[AnyStr] = None):
        """
        :param nodes: the nodes of the pipeline
        :param name: the name of the pipeline
        """
        self._graph = self.resolve_graph(nodes)
        self._name = name

    @property
    def name(self) -> str:
        """
        the name of the pipeline
        :return: string of the name
        """
        return self._name

    def set_pipeline_name(self, name: str):
        """
        sets the name of the pipeline
        :param name: the desired name of the pipeline
        """
        self._name = name

    @classmethod
    def resolve_graph(cls, nodes: List[Node]) -> List[Node]:
        """
        transforms a user provided graph into a usable graph: checking linkage, replacing symbolic references by
        real ones, ...

        :param nodes: the list of linked nodes
        :return: the transformed list of linked nodes
        """
        symbolic_to_real_node_map = cls._build_symbolic_real_node_mapping(nodes)
        real_nodes = [node.replace_symbolic_references(symbolic_to_real_node_map) for node in nodes]
        cls._check_graph(real_nodes)
        return real_nodes

    @staticmethod
    def _build_symbolic_real_node_mapping(nodes: List[Node]) -> SymbolicToRealMapping:
        """
        builds a mapping of nodes with their symbolic name in key and the object in value

        :param nodes: the nodes to build the mapping from
        :return: the mapping
        """
        symbolic_to_real_mapping = {node.output_node: node for node in nodes if node.output_node}
        symbolic_to_real_mapping.update({node.value: node for node in ReservedNodes})
        return symbolic_to_real_mapping

    @classmethod
    def _check_graph(cls, nodes: List[Node]):
        """
        checks a graph for potential problems.
        raises if a node's input is not in the graph or if a node is used twice in the pipeline

        :param nodes: the nodes to check
        """
        available_nodes = {ReservedNodes.pipeline_input}
        for node in nodes:
            available_nodes = cls._update_ancestry(node, available_nodes)

    @classmethod
    def _update_ancestry(cls, node: Node, available_nodes: Set[Node]) -> Set[Node]:
        """
        updates the list of available nodes with a node of interest if possible

        :param node: the node of interest
        :param available_nodes: the available nodes to date
        :return:  the updated ancestry
        """
        orphan_nodes = [input_node for input_node in node.input_nodes if input_node not in available_nodes]
        if orphan_nodes:
            raise ValueError(f"cannot find node(s) {orphan_nodes} in ancestry")
        if node in available_nodes:
            raise ValueError("can only use a node in a graph")
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
        """
        extracts the output of a pipeline.
        raises ValueError if some output was unused once every node is computed and the remaining is not the output of
        the pipeline

        :param results: the outputs left unused once the graph has ran
        :return: the result
        """
        node, output = next(iter(results.items()))
        if node.output_node != ReservedNodes.pipeline_output.value:
            raise ValueError("received an output that is not a pipeline output")
        return output

    def get_pipeline_versions(self) -> Mapping[Node, Version]:
        """
        builds the version with ancestry of every node in the pipeline

        :return: the mapping version for node
        """
        versions = {ReservedNodes.pipeline_input: Version()}
        for node in self._graph:
            versions[node] = node.get_version_with_ancestry(versions)
        versions.pop(ReservedNodes.pipeline_input, None)
        return versions

    def load(self, saver: Saver):
        """
        loads this pipeline as last saved in saver

        :param saver: the saver to look for a persisted version of this pipeline into
        :return: this pipeline loaded
        """
        persisted_versions = self._load_versions(saver)

        new_graph = self._graph
        for i, node in enumerate(self._graph):
            new_node = self._load_single_node(node, persisted_versions, saver)
            new_graph[i] = new_node
            self._graph = new_graph
        return self

    def _load_single_node(self, node: Node, versions: Mapping[Node, List[Version]], saver: Saver):
        """
        loads a single node as persisted in saver (with the last compatible version) if possible

        :param node: the node to load
        :param versions: the node to version mapping of the pipeline
        :param saver: the saver to load the node from
        :return: the loaded node
        """
        if not node.is_loadable:
            return node
        node.check_version(persisted_versions=versions, current_versions=self.get_pipeline_versions())
        node_path = self._get_path_from_versions(versions, node)
        return node.load(saver, node_path)

    def _load_versions(self, saver: Saver) -> Mapping[Node, List[Version]]:
        """
        loads the versions of nodes as they were previously saved

        :param saver: the saver to look for old pipelines in
        :return: the mapping of list of versions per node
        """
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
    def pipeline_meta_path(self) -> str:
        """
        generates the path of the meta of this op

        :return: the string of the path
        """
        return os.path.join(PIPELINE_PATH, self.name, "_meta.json")

    @staticmethod
    def _get_path_from_versions(versions: Mapping[Node, List[Version]], node: Node) -> Text:
        """
        generates the path a persisted node should be at on the saver for it's most up to date version

        :param versions: the mapping of version for each node
        :param node: the node to get the path for
        :return: the path
        """
        node_version = max(versions[node])
        return os.path.join(OPS_PATH, node.name, str(node_version))

    @property
    def node_for_name(self) -> Mapping[Text, Node]:
        """
        generates a mapping with each nodes's name in key and the object as value

        :return: the mapping
        """
        return {node.name: node for node in self._graph}

    def save(self, saver: Saver):
        """
        saves this pipeline in saver

        :param saver: the saver to save the pipeline in
        """
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
        """
        saves the metadata of the pipeline in saver

        :param meta: a mapping with all the historic version of each node (node as key)
        :param saver: the saver to save the meta to
        """
        new_meta_bytes = JSONSerializer().serialize_object({
            node.name: [str(version) for version in node_versions]
            for node, node_versions in meta.items()
        })
        saver.save(new_meta_bytes, self.pipeline_meta_path)

    def _persist_nodes(self, saver: Saver, pipeline_versions: Mapping[Node, Version]):
        """
        persists the nodes of the pipeline to the saver

        :param saver: the saver to save the pipeline into
        """
        for node in self._graph:
            if node.is_loadable:
                node.persist(saver, pipeline_versions[node])

    @staticmethod
    def _update_versions(historic_versions: Mapping[Node, List[Version]],
                         pipeline_versions: Mapping[Node, Version], node: Node) -> Mapping[Node, List[Version]]:
        """
        updates the historic versions of the pipeline with the current versions

        :param historic_versions: the historic versions of each node
        :param pipeline_versions: the current pipeline versions (with ancestry) of the ops
        :param node: the node to update the version of in the history
        :return: the updated history
        """
        if not node.is_loadable:
            return historic_versions
        if pipeline_versions[node] in historic_versions[node]:
            return historic_versions
        historic_versions[node].append(pipeline_versions[node])
        return historic_versions
