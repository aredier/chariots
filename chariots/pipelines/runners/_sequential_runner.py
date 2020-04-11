"""sequential runner op module"""
from typing import Optional, Any

from ... import pipelines
from . import BaseRunner


class SequentialRunner(BaseRunner):  # pylint: disable=too-few-public-methods
    """
    runner that executes every node in a pipeline sequentially in a single thread.
    """

    def run(self, pipeline: 'pipelines.Pipeline', pipeline_input: Optional[Any] = None):

        for callback in pipeline.callbacks:
            callback.before_execution(pipeline, [pipeline_input])
        temp_results = {
            pipelines.nodes.ReservedNodes.pipeline_input.reference: pipeline_input
        } if pipeline_input is not None else {}

        for node in pipeline.pipeline_nodes:
            temp_results = pipeline.execute_node(node, temp_results, self)

        results = {key: value for key, value in temp_results.items() if value is not None}
        if len(results) > 1:
            raise ValueError('multiple pipeline outputs cases not handled, got {}'.format(results))

        temp_results = pipeline.extract_results(temp_results)
        for callback in pipeline.callbacks:
            callback.after_execution(pipeline, [pipeline_input], temp_results)
        return temp_results
