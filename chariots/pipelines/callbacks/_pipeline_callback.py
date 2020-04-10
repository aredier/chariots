"""module  for Pipeline callbacks"""
from typing import List, Any


class PipelineCallback:
    """
    a pipeline callback is used to define instructions that need to be executed at certain points in the pipeline
    execution:

    - before the pipeline is ran
    - before each node of the pipeline
    - after each node of the pipeline
    - after the pipeline is ran

    to create your own, you need to overide one or more of the `before_execution`, `after_execution`,
    `before_node_execution`, `after_node_execution` methods:

    .. testsetup::

        >>> from chariots import Pipeline
        >>> from chariots.callbacks import PipelineCallback
        >>> from chariots.nodes import Node
        >>> from chariots.runners import SequentialRunner
        >>> from chariots._helpers.doc_utils import IsOddOp, AddOneOp

        >>> runner = SequentialRunner()

    .. doctest::

        >>> class MyPipelineLogger(PipelineCallback):
        ...
        ...     def before_execution(self, pipeline: "chariots.Pipeline", args: List[Any]):
        ...         print('running {}'.format(pipeline))
        ...
        ...     def before_node_execution(self, pipeline: "chariots.Pipeline", node: "BaseNode", args: List[Any]):
        ...         print('running {} for {}'.format(node.name, pipeline.name))

    .. doctest::

        >>> is_even_pipeline = Pipeline([
        ...     Node(AddOneOp(), input_nodes=['__pipeline_input__'], output_nodes='modified'),
        ...     Node(IsOddOp(), input_nodes=['modified'],
        ...          output_nodes=['__pipeline_output__'])
        ... ], 'simple_pipeline', pipeline_callbacks=[MyPipelineLogger()])
        >>> runner.run(is_even_pipeline, 3)
        running <OP simple_pipeline>
        running addoneop for simple_pipeline
        running isoddop for simple_pipeline
        False
    """

    def before_execution(self, pipeline: 'chariots.Pipeline', args: List[Any]):
        """
        called before any node in the pipeline is ran. provides the pipeline that is being run and the pipeline input

        :param pipeline: the pipeline being ran
        :param args: the pipeline inputs. DO NOT MODIFY those references as this might cause some undefined behavior
        """

    def after_execution(self, pipeline: 'chariots.Pipeline', args: List[Any], output: Any):
        """
        called after all the nodes of the pipeline have been ran with the pipeline being run and the output of the run

        :param pipeline: the pipeline being run
        :param args: the pipeline input that as given at the beginning of the run
        :param output: the output of the pipeline run. DO NOT MODIFY those references as this might cause some
                       undefined behavior
        """

    def before_node_execution(self, pipeline: 'chariots.Pipeline', node: 'BaseNode', args: List[Any]):
        """
        called before each node is executed the pipeline the node is in as well as the node are provided alongside the
        arguments the node is going to be given

        :param pipeline: the pipeline being run
        :param node: the node that is about to run
        :param args: the arguments that are going to be given to the node. DO NOT MODIFY those references as this might
                     cause some undefined behavior
        """

    def after_node_execution(self, pipeline: 'chariots.Pipeline', node: 'BaseNode', args: List[Any], output: Any):
        """
        called after each node is executed. The pipeline the node is in as well as the node are provided alongside the
        input/output of the node that ran

        :param pipeline: the pipeline being run
        :param node: the node that is about to run
        :param args: the arguments that was given to the node
        :param output: the output the node produced. . DO NOT MODIFY those references as this might cause some
                       undefined behavior
        """
