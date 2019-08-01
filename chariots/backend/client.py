import json
import os
from abc import abstractmethod, ABC
from typing import Any, Optional, Mapping

import requests

from chariots.backend.app import PipelineResponse, Chariot
from chariots.core.nodes import Node
from chariots.core.pipelines import Pipeline
from chariots.core.versioning import Version
from chariots.helpers.errors import VersionError


class AbstractClient(ABC):

    def _send_request_to_backend(self, route: str, data: Optional[Any] = None, method: str = "post") -> Any:
        """
        sends a request to the backend and checks for the relevant error codes
        :param route: the route to request
        :param data: the data to send to the backend (must be JSON serializable)
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
            raise ValueError("the execution of the pipeline failed, see backend logs for traceback")
        if code == 419:
            raise VersionError("the pipeline you requested cannot be loaded because of version incompatibility"
                               "HINT: retrain and save/reload in order to have a loadable version")

    def request(self, pipeline: Pipeline, pipeline_input: Optional[Any] = None) -> PipelineResponse:
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
        sends a request and unwraps the result

        :param pipeline: the pipeline to call
        :param pipeline_input: the data to call the pipeline on
        :return: the result of the pipeline
        """
        return self.request(pipeline, pipeline_input).value

    def save_pipeline(self, pipeline: Pipeline):
        """
        saves the current state of the pipeline

        :param pipeline: the pipeline to save
        """
        save_route = "/pipelines/{}/save".format(pipeline.name)
        self._send_request_to_backend(save_route)

    def load_pipeline(self, pipeline: Pipeline):
        """
        reloads the pipeline (for instance if another pipeline saved an op)

        :param pipeline: the pipeline to reload
        """
        load_route = "/pipelines/{}/load".format(pipeline.name)
        self._send_request_to_backend(load_route)

    def is_pipeline_loaded(self, pipeline: Pipeline) -> bool:
        """
        checks if the pipeline managed to load properly at setup

        :param pipeline: the pipeline to load
        """
        check_route = "/pipelines/{}/health_check".format(pipeline.name)
        response = self._send_request_to_backend(check_route, method="get")
        return response["is_loaded"]

    def pipeline_versions(self, pipeline: Pipeline) -> Mapping[Node, Version]:
        """
        gets the upstream versions of all the nodes in the pipeline

        :param pipeline: the pipeline to get the versions for
        :return:  mapping with the nodes in keys and the versions in values
        """
        versions_route = "/pipelines/{}/versions".format(pipeline.name)
        raw_mapping = self._send_request_to_backend(versions_route)
        return {
            pipeline.node_for_name[node_name]: {key: Version.parse(value) for key, value in version_json.items()}
            for node_name, version_json in raw_mapping.items()
        }


class Client(AbstractClient):
    """
    The Chariot Client is the way to interface with the backend runing the pipelines in the background
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

    def __init__(self, app: Chariot):
        self._test_client = app.test_client()

    def _post(self, route: str, data: Any):
        response = self._test_client.post(route, data=json.dumps(data), content_type='application/json')
        self._check_code(response.status_code)
        return json.loads(response.data)

    def _get(self, route: str, data: Any):
        response = self._test_client.ge(route)
        self._check_code(response.status_code)
        return json.loads(response.data)
