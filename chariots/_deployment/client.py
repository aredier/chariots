import json
from abc import abstractmethod, ABC
from typing import Any, Optional, Mapping

import requests

from chariots._deployment.app import PipelineResponse, Chariots
from chariots import Pipeline
from chariots.versioning import Version
from chariots.errors import VersionError


class AbstractClient(ABC):
    """
    base class for the chariots Clients. it defines the base behaviors and routes available
    """

    def _send_request_to_backend(self, route: str, data: Optional[Any] = None, method: str = "post") -> Any:
        """
        sends a request to the _deployment and checks for the relevant error codes
        :param route: the route to request
        :param data: the data to send to the _deployment (must be JSON serializable)
        :return: the response of the request
        """
        if method == "post":
            return self._post(route, data)
        if method == "get":
            return self._get(route, data)
        raise ValueError("unhandled method: {}".format(method))

    @abstractmethod
    def _get(self, route: str, data: Any):
        pass

    @abstractmethod
    def _post(self, route: str, data: Any):
        pass

    @staticmethod
    def _check_code(code):
        if code == 404:
            raise ValueError("the pipeline you requested is not present on the app")
        if code == 500:
            raise ValueError("the execution of the pipeline failed, see _deployment logs for traceback")
        if code == 419:
            raise VersionError("the pipeline you requested cannot be loaded because of version incompatibility"
                               "HINT: retrain and save/reload in order to have a loadable version")

    def _request(self, pipeline: Pipeline, pipeline_input: Optional[Any] = None) -> PipelineResponse:
        """
        sends a request to execute a pipeline on an input and returns the response

        :param pipeline: the pipeline to execute
        :param pipeline_input: the input to execute the pipeline on
        :return: a response contaning the versions of the nodes in the pipeline and the value of the executed pipeline
        """
        pipe_route = "/pipelines/{}/main".format(pipeline.name)
        response_json = self._send_request_to_backend(route=pipe_route, data={"pipeline_input": pipeline_input})
        return PipelineResponse.from_request(response_json, pipeline)

    def call_pipeline(self, pipeline: Pipeline, pipeline_input: Optional[Any] = None) -> Any:
        """
        sends a request to the `Chariots` server in order to get this pipeline executed remotely on the server.

        .. testsetup::

            >>> import tempfile
            >>> import shutil

            >>> from chariots import Pipeline, Chariots, TestClient
            >>> from chariots._helpers.doc_utils import is_odd_pipeline
            >>> app_path = tempfile.mkdtemp()
            >>> app = Chariots([is_odd_pipeline], app_path, import_name='simple_app')
            >>> client = TestClient(app)

        .. doctest::

            >>> client.call_pipeline(is_odd_pipeline, 4)
            False
            >>> client.call_pipeline(is_odd_pipeline, 5)
            True

        .. testsetup::
            >>> shutil.rmtree(app_path)

        here you can get the user gets the output of the pipeline that got executed in our `Chariots` micro service

        :param pipeline: the pipeline that needs to be executed in the remote `Chariots` server
        :param pipeline_input: the input of the pipeline (will be provided to the node with `__pipeline__input__` in
                               it's `input_nodes`). If none of the nodes accept a __pipeline_input__ and this is
                               provided the execution of the pipeline will fail. pipeline_input needs to be JSON
                               serializable

        :raises ValueError: if the pipeline requested is not present in the `Chariots` app.
        :raises ValueError: if the execution of the pipeline fails

        :return: the result of the pipeline. it needs to be JSON serializable for chariots to be able to pass it
                 through http
        """
        return self._request(pipeline, pipeline_input).value

    def save_pipeline(self, pipeline: Pipeline):
        """
        persists the state of the pipeline on the remote `Chariots` server (usually used for saving the nodes that were
        trained in a train pipeline in order to load them inside the inference pipelines).

        :param pipeline: the pipeline to save on the remote server. Beware: any changes made to the `pipeline` param
                         will not be persisted (Only changes made on the remote version of the pipeline)
        """
        save_route = "/pipelines/{}/save".format(pipeline.name)
        self._send_request_to_backend(save_route)

    def load_pipeline(self, pipeline: Pipeline):
        """
        reloads all the nodes in a pipeline. this is usually used to load the updates of a node/model in the inference
        pipeline after the training pipeline(s) have been executed. If the latest version of a saved node is
        incompatible with the rest of the pipeline, this will raise a `VersionError`

        :param pipeline: the pipeline to reload

        :raises VersionError: If there is a version incompatibility between one of the nodes in the pipeline and one of
                              it's inputs
        """
        load_route = "/pipelines/{}/load".format(pipeline.name)
        self._send_request_to_backend(load_route)

    def is_pipeline_loaded(self, pipeline: Pipeline) -> bool:
        """
        checks whether or not the pipeline has been loaded

        :param pipeline: the pipeline to check
        """
        check_route = "/pipelines/{}/health_check".format(pipeline.name)
        response = self._send_request_to_backend(check_route, method="get")
        return response["is_loaded"]

    def pipeline_versions(self, pipeline: Pipeline) -> Mapping[str, Version]:
        """
        gets all the versions of the nodes of the pipeline (different from `pipeline.get_pipeline_versions` as the
        client will return the version of the loaded/trained version on the (remote) `Chariots` server)

        :param pipeline: the pipeline to get the versions for
        :return:  mapping with the node names in keys and the version object in value
        """
        versions_route = "/pipelines/{}/versions".format(pipeline.name)
        raw_mapping = self._send_request_to_backend(versions_route)
        return {
            node_name: Version.parse(version_str)
            for node_name, version_str in raw_mapping.items()
        }


class Client(AbstractClient):
    """
    Client to query/save/load the pipelines served by a (remote) `Chariots` app.

    for instance if you have built your app as such and deployed it:

    .. testsetup::

        >>> import tempfile
        >>> import shutil

        >>> from chariots import Pipeline, Chariots, MLMode, TestClient
        >>> from chariots.nodes import Node
        >>> from chariots._helpers.doc_utils import IrisXDataSet, PCAOp, IrisFullDataSet, LogisticOp

        >>> app_path = tempfile.mkdtemp()

    .. doctest::

        >>> train_pca = Pipeline([Node(IrisXDataSet(), output_nodes=["x"]), Node(PCAOp(mode=MLMode.FIT),
        ...                       input_nodes=["x"])], "train_pca")

        >>> train_logistic = Pipeline([
        ...     Node(IrisFullDataSet(), output_nodes=["x", "y"]),
        ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["x"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.FIT), input_nodes=["x_transformed", "y"])
        ... ], 'train_logistics')

        >>> pred = Pipeline([
        ...     Node(PCAOp(MLMode.PREDICT), input_nodes=["__pipeline_input__"], output_nodes="x_transformed"),
        ...     Node(LogisticOp(MLMode.PREDICT), input_nodes=["x_transformed"], output_nodes="__pipeline_output__")
        ... ], "pred")

        >>> app = Chariots([train_pca, train_logistic, pred], app_path, import_name="iris_app")

    .. testsetup::

        >>> client = TestClient(app)

    you can then train save and load your pipelines remotely from the client

    .. doctest::

        >>> client.call_pipeline(train_pca)
        >>> client.save_pipeline(train_pca)
        >>> client.load_pipeline(train_logistic)
        >>> client.call_pipeline(train_logistic)
        >>> client.save_pipeline(train_logistic)
        >>> client.load_pipeline(pred)
        >>> client.call_pipeline(pred, [[1, 2, 3, 4]])
        [1]

    but if you execute them in the wrong order the client will propagate the errors that occur on the `Chariots` server

    .. doctest::

        >>> client.call_pipeline(train_pca)
        >>> client.save_pipeline(train_pca)
        >>> client.load_pipeline(pred)
        Traceback (most recent call last):
        ...
        chariots.errors.VersionError: the pipeline you requested cannot be loaded because of version \
incompatibilityHINT: retrain and save/reload in order to have a loadable version

    .. testsetup::
        >>> shutil.rmtree(app_path)

    this example is overkill as you can use `MLMode.FitPredict` flag (not used here to demonstrate the situations where
    `VersionError` will be raised). this would reduce the amount of saving/loading to get to the prediction.
    """

    def __init__(self, backend_url: str = "http://127.0.0.1:5000"):
        self.backend_url = backend_url

    def _post(self, route, data) -> Any:
        response = requests.post(
            self._format_route(route),
            headers={"Content-Type": "application/json"},
            data=json.dumps(data)
        )
        self._check_code(response.status_code)
        return response.json()

    def _get(self, route, data) -> Any:
        if data is not None:
            raise ValueError("get unhandled with data")
        response = requests.get(url=self._format_route(route))
        self._check_code(response.status_code)
        return response.json()

    def _format_route(self, route):
        return self.backend_url + route


class TestClient(AbstractClient):
    """mock up of the client to test a full app without having to create a server"""

    def __init__(self, app: Chariots):
        self._test_client = app.test_client()

    def _post(self, route: str, data: Any):
        response = self._test_client.post(route, data=json.dumps(data), content_type='application/json')
        self._check_code(response.status_code)
        return json.loads(response.data.decode("utf-8"))

    def _get(self, route: str, data: Any):
        response = self._test_client.ge(route)
        self._check_code(response.status_code)
        return json.loads(response.data.decode("utf-8"))
