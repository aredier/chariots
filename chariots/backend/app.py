import json
from typing import Text, Mapping, Any

from flask import Flask, request
from requests import Response

from chariots.core.pipelines import Pipeline, SequentialRunner, Node
from chariots.core.versioning import Version


class PipelineResponse(object):

    def __init__(self, value: Any, versions: Mapping[Node, Version]):
        self.value = value
        self.versions = versions

    def json(self):
        return {
            "pipeline_output": self.value,
            "versions": {node.name: str(version) for node, version in self.versions.items()}
        }

    @classmethod
    def from_request(cls, response: Response, query_pipeline: Pipeline):
        if not response.status_code == 200:
            raise ValueError("trying to parse non nominal response")
        response_json = response.json()
        return cls(
            value=response_json["pipeline_output"],
            versions={query_pipeline.node_for_name[node_name]: Version.parse(version_string)
                      for node_name, version_string in response_json["versions"].items()}
        )


class Chariot(Flask):

    def __init__(self, pipelines: Mapping[Text, Pipeline], *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._build_routes(pipelines)

    def _build_routes(self, pipelines):
        for pipeline_name, pipeline in pipelines.items():
            self.add_url_rule(f"/pipes/{pipeline_name}", pipeline_name,
                              self._build_endpoint_from_pipeline(pipeline),
                              methods=['POST'])

    @staticmethod
    def _build_endpoint_from_pipeline(pipeline: Pipeline):

        def inner():
            pipeline_input = request.json.get("pipeline_input") if request.json else None
            print(request.json)
            response = PipelineResponse(pipeline(SequentialRunner(), pipeline_input), pipeline.get_pipeline_versions())
            return json.dumps(response.json())
        return inner
