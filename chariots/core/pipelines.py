import os
import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Mapping, Text, Set, Any, Dict, Optional, Union, Tuple, AnyStr

from chariots.core import nodes
from chariots.core.ops import AbstractOp, OPS_PATH
from chariots.core.saving import Saver, JSONSerializer
from chariots.core.versioning import Version
from chariots.helpers.typing import ResultDict, SymbolicToRealMapping

PIPELINE_PATH = "/pipelines"


class _OpStore:
    """
    The op store abstracts the saving of ops and their relevant versions.
    """

    # the format of the app's op repo is:
    # {
    #     op_name: {
    #         pipeline: [
    #             "op_version": the op version
    #             "upstream_version": the upstream version
    #         ]
    #     }
    # }

    _location = "/_meta.json"

    def __init__(self, saver: Saver):
        """
        :param saver: the saver the op_store will use to retrieve it's metadata and subsequent ops
        """
        self._saver = saver
        self._all_op_versions = {}
        self._load_from_saver()

    def _load_from_saver(self):
        """
        loads and parses all the versions from the meta json
        """
        try:
            mapping = json.loads(self._saver.load(path=self._location))
            self._all_op_versions = self._parse_mapping(mapping)
        except FileNotFoundError:
            pass

    def _parse_mapping(
            self, op_version_json: Mapping[AnyStr, Union[Mapping, List[Mapping[AnyStr, AnyStr]]]]
    ) -> Mapping[AnyStr, Union[Mapping, List[Mapping[AnyStr, Version]]]]:
        """
        parses the saved meta and returns a valid metadata (with versions as object rather than strings)

        :param op_version_json: all the op version metadata in json format
        :return: the op version metadata
        """
        for key, value in op_version_json.items():
            if isinstance(value, dict):
                op_version_json[key] = self._parse_mapping(value)
            if isinstance(value, list):
                op_version_json[key] = [
                    {k: Version.parse(v) for k, v in version_dict.items()}
                    for version_dict in value
                ]
            return op_version_json

    def save(self):
        """
        persists the the op version metadata.
        """
        self._saver.save(json.dumps(self._all_op_versions), path=self._location)

    def get_all_verisons_of_op(self, op: AbstractOp, fallback: Any = None) -> Optional[List[Version]]:
        """
        gets all the versions of an op that were previously persisted (the op version and not the upstream one)
        regardless of which pipeline saved it

        :param op: the op to get the previous persisted versions
        :param fallback: the fallback to give back if the op has never been persisted
        """

        op_data = self._all_op_versions.get(op.name, None)
        if op_data is None:
            return fallback
        return [version_dict["op_version"] for pipeline_data in op_data.values() for version_dict in pipeline_data]

    def get_last_op_versions_from_pipeline(
            self, op: AbstractOp, pipeline: "Pipeline", fallback: Any = None
    ) -> Optional[Tuple[Version, Version]]:
        """
        get's the latest versions (both op version and upstream version) of an op inside of a pipeline (upstream of the
        same pipeline in previous saves)

        :param op: the op to find the versions for
        :param pipeline: the pipeline that was used to save this op
        :param fallback: a fallback to return in case no op was found saved with this pipeline
        :return: the op version and the upstram version (in that order)
        """

        pipe_versions = self._all_op_versions.get(op.name, {}).get(pipeline.name, None)
        if pipe_versions is None:
            return fallback
        return pipe_versions[-1]["op_version"], pipe_versions[-1]["upstream_version"]

    def get_op_bytes_for_version(self, op: AbstractOp, version: Version) -> bytes:
        """
        loads the persisted bytes of an op given the version that needs loading

        :param op: the op to laod
        :param version: the version of the op to load
        :return: the bytes of the op
        """
        path = self._build_op_path(op.name, version)
        return self._saver.load(path=path)

    @staticmethod
    def _build_op_path(op_name: str, version: Version) -> str:
        """
        builds the path an op should be persisted at given it's version

        :param op_name: the name of the op to build the path for
        :param version: th the version of that op
        :return: the path at which to save
        """

        return "/models/{}/{}".format(op_name, str(version))

    def save_op_bytes_for_pipeline(self, op: AbstractOp, pipeline_name: str, pipeline_version: Version,
                                   op_bytes: bytes):
        """
        saves a loadable op present in pipeline

        :param op: the op to save
        :param pipeline_version: the upstream version of the op to save
        :param pipeline_name: the name of the op to save
        :param op_bytes: the bytes of the op to save
        """
        self._all_op_versions.setdefault(
            op.name, {}
        ).setdefault(
            pipeline_name, []
        ).append({
            "op_version": op.__version__,
            "upstream_version": pipeline_version
        })
        path = self._build_op_path(op.name, version=op.__version__)
        self._saver.save(serialized_object=op_bytes, path=path)

    def link_pipeline_to_op_version(self, op: AbstractOp, op_version: Version, pipeline_version: Version,
                                    pipeline_name: str):
        if self.get_all_verisons_of_op(op, None) is None:
            raise ValueError("cannot link op {} to version {} as it doesn't exist "
                             "in the store".format(op.name, op_version))
        self._all_op_versions[op.name].setdefault(pipeline_name, []).append({
            "op_version": op.__version__,
            "upstream_version": pipeline_version
        })


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
        temp_results = {ReservedNodes.pipeline_input: pipeline_input} if pipeline_input else {}
        for node in graph:
            temp_results = self._execute_node(node, temp_results)
        return temp_results

    def _execute_node(self, node: "nodes.AbstractNode", temp_results: ResultDict) -> ResultDict:
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
        symbolic_to_real_mapping = {node.output_node: node for node in pipeline_nodes if node.output_node}
        symbolic_to_real_mapping.update({node.value: node for node in ReservedNodes})
        return symbolic_to_real_mapping

    @classmethod
    def _check_graph(cls, pipeline_nodes: List["nodes.AbstractNode"]):
        """
        checks a graph for potential problems.
        raises if a node's input is not in the graph or if a node is used twice in the pipeline

        :param pipeline_nodes: the nodes to check
        """
        available_nodes = {ReservedNodes.pipeline_input}
        for node in pipeline_nodes:
            available_nodes = cls._update_ancestry(node, available_nodes)

    @classmethod
    def _update_ancestry(cls, node: "nodes.AbstractNode",
                         available_nodes: Set["nodes.AbstractNode"]) -> Set["nodes.AbstractNode"]:
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
    def extract_results(results: Dict["nodes.AbstractNode", Any]) -> Any:
        """
        extracts the output of a pipeline.
        raises ValueError if some output was unused once every node is computed and the remaining is not the output of
        the pipeline

        :param results: the outputs left unused once the graph has ran
        :return: the result
        """
        node, output = next(iter(results.items()))
        if output is not None and node.output_node != ReservedNodes.pipeline_output.value:
            raise ValueError("received an output that is not a pipeline output")
        return output

    def get_pipeline_versions(self) -> Mapping["nodes.AbstractNode", Version]:
        """
        builds the version with ancestry of every node in the pipeline

        :return: the mapping version for node
        """
        versions = {ReservedNodes.pipeline_input: Version()}
        for node in self._graph:
            versions[node] = node.get_version_with_ancestry(versions)
        versions.pop(ReservedNodes.pipeline_input, None)
        return versions

    def load(self, op_store: _OpStore):
        """
        loads this pipeline as last saved in saver

        :type op_store: the op store to collect the ops and versions from
        :return: this pipeline loaded
        """

        # the format of the app's op repo is:
        # {
        #     op_name: {
        #         pipeline: {
        #             op_version: upstream_version
        #         }
        #     }
        # }

        new_graph = self._graph
        for i, node in enumerate(self._graph):
            new_graph[i] = self._load_single_node(node, op_store)
            self._graph = new_graph
        return self

    def _load_single_node(self, node: "nodes.AbstractNode",  op_store: _OpStore):  # noqa
        """
        loads a single node as persisted in saver (with the last compatible version) if possible

        :param node: the node to load
        :return: the loaded node
        """
        if not node.is_loadable:
            return node
        return node.check_and_load(op_store, self)

    def _load_versions(self, saver: Saver) -> Mapping["nodes.AbstractNode", List[Version]]:
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
    def _get_path_from_versions(versions: Mapping["nodes.AbstractNode", List[Version]],
                                node: "nodes.AbstractNode") -> Text:  # noqa
        """
        generates the path a persisted node should be at on the saver for it's most up to date version

        :param versions: the mapping of version for each node
        :param node: the node to get the path for
        :return: the path
        """
        node_version = max(versions[node])
        return os.path.join(OPS_PATH, node.name, str(node_version))

    @property
    def node_for_name(self) -> Mapping[Text, "nodes.AbstractNode"]:
        """
        generates a mapping with each nodes's name in key and the object as value

        :return: the mapping
        """
        return {node.name: node for node in self._graph}

    def save(self, op_store: _OpStore):
        """
        saves this pipeline in saver

        :param saver: the saver to save the pipeline in
        """
        for node in self._graph:
            if node.is_loadable:
                node.persist(op_store, self)

    @staticmethod
    def _update_versions(historic_versions: Mapping["nodes.AbstractNode", List[Version]],
                         pipeline_versions: Mapping["nodes.AbstractNode", Version],
                         node: nodes.AbstractNode) -> Mapping["nodes.AbstractNode", List[Version]]:
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
