import json
from typing import Text, Mapping, Any, List

import requests
from flask import Flask, request

from chariots.core.pipelines import Pipeline, SequentialRunner, Node
from chariots.core.versioning import Version


class PipelineResponse:
    """
    A PipelineResponse represents all the information that is sent from the backend when a pipeline is executed.
    """

    def __init__(self, value: Any, versions: Mapping[Node, Version]):
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
    """

    def __init__(self, pipelines: List[Pipeline], *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._build_routes(pipelines)

    def _build_routes(self, pipelines):
        for pipeline in pipelines:
            self.add_url_rule(f"/pipes/{pipeline.name}", pipeline.name,
                              self._build_endpoint_from_pipeline(pipeline),
                              methods=['POST'])

    @staticmethod
    def _build_endpoint_from_pipeline(pipeline: Pipeline):

        def inner():
            pipeline_input = request.json.get("pipeline_input") if request.json else None
            response = PipelineResponse(pipeline(SequentialRunner(), pipeline_input), pipeline.get_pipeline_versions())
            return json.dumps(response.json())
        return inner
