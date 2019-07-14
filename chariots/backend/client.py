import json
from typing import Any, Optional

import requests

from chariots.backend.app import PipelineResponse
from chariots.core.pipelines import Pipeline


class Client:
    """
    The Chariot Client is the way to interface with the backend runing the pipelines in the background
    """

    def __init__(self, backend_url: str = "http://127.0.0.1:5000"):
        self.backend_url = backend_url

    def request(self, pipeline: Pipeline, pipeline_input: Optional[Any] = None) -> PipelineResponse:
        """
        sends a request to execute a pipeline on an input and returns the response

        :param pipeline: the pipeline to execute
        :param pipeline_input: the input to execute the pipeline on
        :return: a response contaning the versions of the nodes in the pipeline and the value of the executed pipeline
        """
        pipe_url = "{}/pipelines/{}/main".format(self.backend_url, pipeline.name)
        response = requests.post(
            pipe_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"pipeline_input": pipeline_input})
        )
        if response.status_code == 404:
            raise ValueError("the pipeline you requested is not present on the app")
        if response.status_code == 500:
            raise ValueError("the execution of the pipeline failed, see backend logs for traceback")
        return PipelineResponse.from_request(response, pipeline)

    def call_pipeline(self, pipeline: Pipeline, pipeline_input: Optional[Any] = None) -> Any:
        """
        sends a request and unwraps the result

        :param pipeline: the pipeline to call
        :param pipeline_input: the data to call the pipeline on
        :return: the result of the pipeline
        """
        return self.request(pipeline, pipeline_input).value
