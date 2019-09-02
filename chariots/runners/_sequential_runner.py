from typing import Optional, Any

from chariots import Pipeline
from chariots.base import BaseRunner
from chariots.nodes import ReservedNodes


class SequentialRunner(BaseRunner):
    """
    runner that executes every node in a pipeline sequentially in a single thread.
    """

    def run(self, pipeline: "Pipeline", pipeline_input: Optional[Any] = None):

        for callback in pipeline.callbacks:
            callback.before_execution(pipeline, [pipeline_input])
        temp_results = {ReservedNodes.pipeline_input.reference: pipeline_input} if pipeline_input else {}
        for node in pipeline.nodes:
            temp_results = pipeline.execute_node(node, temp_results, self)

        results = {key: value for key, value in temp_results.items() if value is not None}
        if len(results) > 1:
            raise ValueError("multiple pipeline outputs cases not handled, got {}".format(temp_results))

        temp_results = pipeline.extract_results(temp_results)
        for callback in pipeline.callbacks:
            callback.after_execution(pipeline, [pipeline_input], temp_results)
        return temp_results
