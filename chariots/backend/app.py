import json
from typing import Mapping, Any, List, Type

import requests
from flask import Flask, request

from chariots.core.pipelines import Pipeline, SequentialRunner
from chariots.core.versioning import Version
from chariots.core.nodes import AbstractNode
from chariots.core.saving import Saver, FileSaver


class PipelineResponse:
    """
    A PipelineResponse represents all the information that is sent from the backend when a pipeline is executed.
    """

    def __init__(self, value: Any, versions: Mapping[AbstractNode, Version]):
        self.value = value
        self.versions = versions

    def json(self) -> Mapping[str, Any]:
        """
        jsonify the response to be passed over http

        :return: the dict representing this response
        """
        return {
            "pipeline_output": self.value,
            "versions": {node.name: str(version) for node, version in self.versions.items()}
        }

    @classmethod
    def from_request(cls, response: requests.Response, query_pipeline: Pipeline) -> "PipelineResponse":
        """
        builds the response from the response that was received through http and the pipeline used to query it

        :param response: the response of the call
        :param query_pipeline: the pipeline that was used in the query that generated the response
        :return: the corresponding PipelineResponse
        """
        if not response.status_code == 200:
            raise ValueError("trying to parse non nominal response")
        response_json = response.json()
        return cls(
            value=response_json["pipeline_output"],
            versions={query_pipeline.node_for_name[node_name]: Version.parse(version_string)
                      for node_name, version_string in response_json["versions"].items()}
        )


class Chariot(Flask):
    """
    the backend app used to run the pipelines
    for each pipeline, sevreal routes will be built:
    - /pipelines/<pipeline_name>/main
    - /pipelines/<pipeline_name>/versions
    - /pipelines/<pipeline_name>/load
    - /pipelines/<pipeline_name>/save
    - /piepleines/<pipeline_name>/health_check
    as well as some common routes
    - /health_check
    - /available_pipelines
    """

    def __init__(self, pipelines: List[Pipeline], path: str, saver_cls: Type[Saver] = FileSaver, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.saver = saver_cls(path)
        pipelines = self._prepare_pipelines(pipelines)

        self._pipelines = {
            pipe.name: pipe for pipe in pipelines
        }

        self._loaded_pipelines = {
            pipe.name: False for pipe in pipelines
        }

        self._load_pipelines()
        self._build_route()

    def _prepare_pipelines(self, pipelines: List[Pipeline]):
        for pipe in pipelines:
            pipe.prepare(self.saver)
        return pipelines

    def _build_route(self):

        @self.route("/pipelines/<pipeline_name>/main", methods=["POST"])
        def serve_pipeline(pipeline_name):
            if not self._loaded_pipelines[pipeline_name]:
                raise ValueError("pipeline not loaded, load before execution")
            pipeline = self._pipelines[pipeline_name]
            pipeline_input = request.json.get("pipeline_input") if request.json else None
            response = PipelineResponse(pipeline(SequentialRunner(), pipeline_input), pipeline.get_pipeline_versions())
            return json.dumps(response.json())

        @self.route("/pipelines/<pipeline_name>/load", methods=["POST"])
        def load_pipeline(pipeline_name):
            self._load_single_pipeline(pipeline_name)
            return json.dumps({})

        @self.route("/pipelines/<pipeline_name>/versions", methods=["POST"])
        def pipeline_versions(pipeline_name):
            pipeline = self._pipelines[pipeline_name]
            versions = pipeline.get_pipeline_versions()
            version_json = {node.name: str(version) for node, version in versions.items()}
            return json.dumps(version_json)

        @self.route("/pipelines/<pipeline_name>/save", methods=["POST"])
        def save_pipeline(pipeline_name):
            pipeline = self._pipelines[pipeline_name]
            pipeline.save(self.saver)
            return json.dumps({})

        @self.route("/pipelines/<pipeline_name>/health_check", methods=["GET"])
        def pipeline_health_check(pipeline_name):
            is_loaded = self._loaded_pipelines[pipeline_name]
            return json.dumps({"is_loaded": is_loaded}), 200 if is_loaded else 419

        @self.route("/health_check", methods=["GET"])
        def health_check():
            return json.dumps(self._loaded_pipelines)

        @self.route("/available_pipelines", methods=["GET"])
        def all_pipelines():
            return json.dumps(list(self._pipelines.keys()))

    def _load_pipelines(self):
        for pipeline in self._pipelines.values():
            try:
                self._load_single_pipeline(pipeline.name)
            except ValueError:
                continue

    def _load_single_pipeline(self, pipeline_name):
        self._pipelines[pipeline_name].load(self.saver)
        self._loaded_pipelines[pipeline_name] = True
