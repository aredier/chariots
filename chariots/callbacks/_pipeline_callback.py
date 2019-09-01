from typing import List, Any

from chariots import _pipeline


class PipelineCallback:
    """
    a pipeline callback is used to define instructions that need to be executed at certain points in the pipeline
    execution:

    - before the pipeline is ran
    - before each node of the pipeline
    - after each node of the pipeline
    - after the pipeline is ran
    """

    def before_execution(self, pipeline: "_pipeline.Pipeline", args: List[Any]):
        """
        called before any node in the pipeline is ran. provides the pipeline that is being run and the pipeline input

        :param pipeline: the piepline being ran
        :param args: the pipeline inputs
        """
        pass

    def after_execution(self, pipeline: "_pipeline.Pipeline", args: List[Any], output: Any):
        """
        called after all the nodes of the pipeline have been ran with the pipeline being run and the output of the run

        :param pipeline: the pipeline being run
        :param args: the pipeline input that as given at the beginning of the run
        :param output: the output of the pipeline run
        """
        pass

    def before_node_execution(self, pipeline: "_pipeline.Pipeline", node: "nodes.AbstractNode", args: List[Any]):
        """
        called before each node is executed the pipeline the node is in as well as the node are provided alongside the
        arguents the node is going to be given

        :param pipeline: the pipeline being run
        :param node: the node that is about to run
        :param args: the arguments that are going to be given to the node
        """
        pass

    def after_node_execution(self, pipeline: "_pipeline.Pipeline", node: "nodes.AbstractNode", args: List[Any], output: Any):
        """
        called after each node is executed. The pipeline the node is in as well as the node are provided alongside the
        input/output of the node that ran

        :param pipeline: the pipeline being run
        :param node: the node that is about to run
        :param args: the arguments that was given to the node
        :param output: the output the node produced
        """
        pass