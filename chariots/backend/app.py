import json
from typing import Text, Mapping

from flask import Flask, request

from chariots.core.pipelines import Pipeline, SequentialRunner


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
            res = pipeline(SequentialRunner(), pipeline_input)
            return json.dumps(res)
        return inner
