"""module for the abstract runner classes"""
from abc import ABC, abstractmethod
from typing import Optional, Any


class BaseRunner(ABC):  # pylint: disable=too-few-public-methods
    """
    a runner is used to define the execution behavior of a Pipeline. there main entry point is the `run` method

    .. testsetup::

        >>> from chariots.pipelines import Pipeline, PipelinesServer
        >>> from chariots.pipelines.nodes import Node
        >>> from chariots.pipelines.runners import SequentialRunner
        >>> from chariots._helpers.doc_utils import IsOddOp
        >>> is_odd_pipeline = Pipeline([
        ...     Node(IsOddOp(), input_nodes=["__pipeline_input__"], output_nodes=["__pipeline_output__"])
        ... ], "simple_pipeline")
        >>> runner = SequentialRunner()

    .. doctest::

        >>> runner.run(is_odd_pipeline, 3)
        True

    To create a new runner (for instance to execute your pipeline on a cluster) you only have to override `run` method
    and use the `Pipeline`'s class methods (for instance you might want to look at `extract_results`, `execute_node`)
    """

    @abstractmethod
    def run(self, pipeline: 'chariots.pipelines.Pipeline', pipeline_input: Optional[Any] = None):
        """
        runs a pipeline, provides it with the correct input and extracts the results if any

        :param pipeline: the pipeline to run
        :param pipeline_input: the input to be given to the pipeline

        :return: the output of the graph called on the input if applicable
        """
