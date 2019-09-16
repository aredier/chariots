import json
from typing import Mapping, Any, List, Type, Optional

from flask import Flask, request

from chariots import Pipeline
from chariots.base import BaseRunner, BaseSaver, BaseNode
from chariots.callbacks import PipelineCallback
from chariots.errors import VersionError
from chariots.runners import SequentialRunner
from chariots.savers import FileSaver
from chariots.versioning import Version
from .._op_store import OpStore


class PipelineResponse:
    """
    A PipelineResponse represents all the information that is sent from the _deployment when a pipeline is executed.
    """

    def __init__(self, value: Any, versions: Mapping[BaseNode, Version]):
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
    def from_request(cls, response_json: Any, query_pipeline: Pipeline) -> "PipelineResponse":
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


class Chariots(Flask):
    """
    small `Flask` application used to rapidly deploy pipelines:

    .. testsetup::

        >>> import tempfile
        >>> import shutil
        ...
        >>> from chariots import Pipeline, Chariots
        >>> from chariots._helpers.doc_utils import is_odd_pipeline
        >>> app_path = tempfile.mkdtemp()

    .. doctest::

        >>> my_app = Chariots(app_pipelines=[is_odd_pipeline], path=app_path, import_name="my_app")

    .. testsetup::
        >>> shutil.rmtree(app_path)

    you can then deploy the app as you would with the flask comand:

    .. code-block:: console

        $ flask

    or if you have used :doc:`the chariots' template <../template>`, you can use the predefined cli once the project is
    installed:

    .. code-block:: console

        $ my_great_project start

    once the app is started you can use it with the client (that handles creating the requests and serializing to the
    right format) to query your pipelines:

    .. testsetup::

        >>> from chariots import TestClient
        >>> client = TestClient(my_app)

    .. doctest::

        >>> client.call_pipeline(is_odd_pipeline, 4)
        False

    alternatively, you can query the `Chariots` server directly as you would for any normal micro-service. The server
    has the following routes:

    - `/pipelines/<pipeline_name>/main`
    - `/pipelines/<pipeline_name>/versions`
    - `/pipelines/<pipeline_name>/load`
    - `/pipelines/<pipeline_name>/save`
    - `/pipelines/<pipeline_name>/health_check`

    for each pipeline that was registered to the `Chariots` app. It also creates some common routes for all pipelines:

    - `/health_check`
    - `/available_pipelines`

    :param app_pipelines: the pipelines this app will serve
    :param path: the path to mount the app on (whether on local or remote saver). for isntance using a `LocalFileSaver`
                 and '/chariots' will mean all the information persisted by the `Chariots` server (past versions,
                 trained models, datasets) will be persisted there
    :param saver_cls: the saver class to use. if None the `FileSaver` class will be used as default
    :param runner: the runner to use to run the pipelines. If None the `SequentialRunner` will be used
                  as default
    :param default_pipeline_callbacks: pipeline callbacks to be added to every pipeline this app will serve.
    :param args: additional positional arguments to be passed to the Flask app
    :param kwargs: additional keywords arguments to be added to the Flask app

    """

    def __init__(self, app_pipelines: List[Pipeline], path, saver_cls: Type[BaseSaver] = FileSaver,
                 runner: Optional[BaseRunner] = None,
                 default_pipeline_callbacks: Optional[List[PipelineCallback]] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.saver = saver_cls(path)
        self.runner = runner or SequentialRunner()
        self._op_store = OpStore(self.saver)
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

    def _prepare_pipelines(self, app_pipeline: List[Pipeline]):
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
