from typing import Optional, Any

from chariots import Pipeline
from chariots.base import BaseRunner
from chariots.nodes import ReservedNodes


class SequentialRunner(BaseRunner):
    """
    runner that executes a node graph sequentially
    """

    def run(self, pipeline: "Pipeline", pipeline_input: Optional[Any] = None):
        """
        runs a whole pipeline

        :param pipeline_input: the input to be given to the pipeline
        :param pipeline: the pipeline to run

        :return: the output of the graph called on the input if applicable
        """

        for callback in pipeline.callbacks:
            callback.before_execution(pipeline, [pipeline_input])
        temp_results = {ReservedNodes.pipeline_input.reference: pipeline_input} if pipeline_input else {}
        for node in pipeline.nodes:
            temp_results = pipeline.execute_node(node, temp_results, self)

        if len(temp_results) > 1:
            raise ValueError("multiple pipeline outputs cases not handled, got {}".format(temp_results))

        if temp_results is not None:
            temp_results = pipeline.extract_results(temp_results)
        for callback in pipeline.callbacks:
            callback.after_execution(pipeline, [pipeline_input], temp_results)
        return temp_results
