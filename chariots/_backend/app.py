import json
from typing import Mapping, Any, List, Type, Optional

from flask import Flask, request

import chariots._core.op_store
from chariots._core import pipelines
from chariots._core.versioning import Version
from chariots._core.nodes import AbstractNode
from chariots._core.saving import Saver, FileSaver
from chariots.errors import VersionError


class PipelineResponse:
    """
    A PipelineResponse represents all the information that is sent from the _backend when a pipeline is executed.
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
    def from_request(cls, response_json: Any, query_pipeline: pipelines.Pipeline) -> "PipelineResponse":
        """
        builds the response from the response that was received through http and the pipeline used to query it

        :param response_json: the response json of the call
        :param query_pipeline: the pipeline that was used in the query that generated the response
        :return: the corresponding PipelineResponse
        """
        return cls(
            value=response_json["pipeline_output"],
            versions={query_pipeline.node_for_name[node_name]: Version.parse(version_string)
                      for node_name, version_string in response_json["versions"].items()}
        )


class Chariot(Flask):
    """
    the _backend app used to run the pipelines
    for each pipeline, sevreal routes will be built:

    - /pipelines/<pipeline_name>/main
    - /pipelines/<pipeline_name>/versions
    - /pipelines/<pipeline_name>/load
    - /pipelines/<pipeline_name>/save
    - /piepleines/<pipeline_name>/health_check

    as well as some common routes

    - /health_check
    - /available_pipelines

    :param app_pipelines: the pipelines this app will serve
    :param path: the path to mount the app on (whether on local or remote saver)
    :param saver_cls: the saver class to use. if None the `FileSaver` class will be used as default
    :param runner: the runner to use to perform pipelines when saving. If None the `SequentialRunner` will be used
                  as default
    :param default_pipeline_callbacks: pipeline calbacks to be added to every pipeline this app will serve
    :param args: additional positional arguments to be passed to the Flask app
    :param kwargs: additional keywords arguments to be added to the Flask app
    """

    def __init__(self, app_pipelines: List[pipelines.Pipeline], path: str, saver_cls: Type[Saver] = FileSaver,
                 runner: Optional[pipelines.AbstractRunner] = None,
                 default_pipeline_callbacks: Optional[List[pipelines.PipelineCallback]] = None, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.saver = saver_cls(path)
        self.runner = runner or pipelines.SequentialRunner()
        self._op_store = chariots._core.op_store.OpStore(self.saver)
        app_pipelines = self._prepare_pipelines(app_pipelines)

        # adding the default pipeline callbacks to all the pipelines of the app
        for pipeline in app_pipelines:
            pipeline.callbacks.extend(default_pipeline_callbacks or [])

        self._pipelines = {
            pipe.name: pipe for pipe in app_pipelines
        }

        self._loaded_pipelines = {
            pipe.name: False for pipe in app_pipelines
        }

        self._load_pipelines()
        self._build_route()
        self._build_error_handlers()

    def _build_error_handlers(self):
        self.register_error_handler(VersionError, lambda error: error.handle())

    def _prepare_pipelines(self, app_pipeline: List[pipelines.Pipeline]):
        for pipe in app_pipeline:
            pipe.prepare(self.saver)
        return app_pipeline

    def _build_route(self):

        @self.route("/pipelines/<pipeline_name>/main", methods=["POST"])
        def serve_pipeline(pipeline_name):
            if not self._loaded_pipelines[pipeline_name]:
                raise ValueError("pipeline not loaded, load before execution")
            pipeline = self._pipelines[pipeline_name]
            pipeline_input = request.json.get("pipeline_input") if request.json else None
            response = PipelineResponse(self.runner.run(pipeline, pipeline_input),
                                        pipeline.get_pipeline_versions())
            return json.dumps(response.json())

        @self.route("/pipelines/<pipeline_name>/load", methods=["POST"])
        def load_pipeline(pipeline_name):
            self._load_single_pipeline(pipeline_name)
            return json.dumps({})

        @self.route("/pipelines/<pipeline_name>/versions", methods=["POST"])
        def pipeline_versions(pipeline_name):
            pipeline = self._pipelines[pipeline_name]
            return json.dumps({node.name: str(version) for node, version in pipeline.get_pipeline_versions().items()})

        @self.route("/pipelines/<pipeline_name>/save", methods=["POST"])
        def save_pipeline(pipeline_name):
            pipeline = self._pipelines[pipeline_name]
            pipeline.save(self._op_store)
            self._op_store.save()
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
        self._pipelines[pipeline_name].load(self._op_store)
        self._loaded_pipelines[pipeline_name] = True
