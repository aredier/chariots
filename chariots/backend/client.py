import json
from typing import Any, Optional

import requests

from chariots.backend.app import PipelineResponse
from chariots.core.pipelines import Pipeline


class Client:

    def __init__(self, backend_url: str = "http://127.0.0.1:5000"):
        self.backend_url = backend_url

    def request(self, pipeline: Pipeline, pipeline_input: Optional[Any] = None) -> PipelineResponse:
        pipe_url = "{}/pipes/{}".format(self.backend_url, pipeline.name)
        response = requests.post(
            pipe_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"pipeline_input": pipeline_input})
        )
        return PipelineResponse.from_request(response, pipeline)

    def call_pipeline(self, pipeline: Pipeline, pipeline_input: Optional[Any] = None) -> Any:
        return self.request(pipeline, pipeline_input).value
